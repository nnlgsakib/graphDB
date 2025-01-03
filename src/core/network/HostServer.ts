import express from 'express';
import http from 'http';
import cors from 'cors';
import WebSocket from 'ws';
import multer from 'multer';
import logger from '../utils/logger';
import { HashStore, FileMeta } from '../storage/HashStore';
import { ProofStore } from '../storage/ProofStore';
import { chunkBuffer, generateHash, generateRandomHash } from '../utils/cryptoutils';
import { buildMerkleTree } from '../utils/merkleutils';

interface ProviderInfo {
  id: string;
  ws: WebSocket;
  isOnline: boolean;
  latestMerkleRoot: string | null;
  assignedChunks: Set<string>;
}

/**
 * MIME types that we render inline (browser typically supports).
 * Everything else => we force as "attachment".
 */
const INLINE_MIME_TYPES = new Set([
  'image/png',
  'image/jpeg',
  'image/gif',
  'image/webp',
  'text/html',
  'text/plain',
  'application/pdf',
  // Add others if you want them to open in-browser
]);

/**
 * Temporary structure for ongoing "read" requests
 * Key: fileHash
 * Value: An object with:
 *   - totalChunks: number
 *   - chunksReceived: number
 *   - chunkData: Record<chunkHash, Buffer>
 *   - resolve / reject for a Promise
 */
interface ReadRequest {
  totalChunks: number;
  chunksReceived: number;
  chunkData: Record<string, Buffer>;
  resolve: (buf: Buffer) => void;
  reject: (err: Error) => void;
}

export class HostServer {
  private bootPort: number;
  private apiPort: number;

  // The Host no longer uses a DataStore for chunks
  // but we keep hashStore, proofStore for tracking
  private hashStore: HashStore;
  private proofStore: ProofStore;

  private providers: Map<string, ProviderInfo> = new Map();
  private providerCounter = 0;
  private challengeIntervalMs = 5 * 60 * 1000;
  private challengeInterval: NodeJS.Timeout | null = null;

  // Track read requests in progress
  private readRequests: Map<string, ReadRequest> = new Map();

  constructor(bootPort: number, apiPort: number, dbDir: string) {
    this.bootPort = bootPort;
    this.apiPort = apiPort;

    // No DataStore for the Host
    this.hashStore = new HashStore(`${dbDir}/hashes`);
    this.proofStore = new ProofStore(`${dbDir}/proofs`);
  }

  public async start() {
    await this.startWebSocketServer();
    await this.startApiServer();

    // Automatic chunk challenges every 5 minutes
    this.challengeInterval = setInterval(() => {
      this.issueRandomChunkChallenge();
    }, this.challengeIntervalMs);

    logger.info(`HostServer up => bootPort=${this.bootPort}, apiPort=${this.apiPort}`);
  }

  /**
   * Spin up WebSocket server for Provider connections
   */
  private async startWebSocketServer() {
    const server = http.createServer();

    const wss = new WebSocket.Server({ server });

    wss.on('connection', (ws) => {
      const providerId = `provider-${++this.providerCounter}`;
      const provider: ProviderInfo = {
        id: providerId,
        ws,
        isOnline: true,
        latestMerkleRoot: null,
        assignedChunks: new Set<string>(),
      };
      this.providers.set(providerId, provider);

      logger.info(`Provider connected => ${providerId}`);

      // Listen for messages from Provider
      ws.on('message', async (message) => {
        const data = message.toString();
        try {
          const parsed = JSON.parse(data);
          switch (parsed.type) {
            case 'REGISTER_PROVIDER':
              logger.info(`Registered provider => ${providerId}`);
              break;

            case 'MERKLE_UPDATE':
              provider.latestMerkleRoot = parsed.root || null;
              logger.info(`Provider ${providerId} => MERKLE_UPDATE root=${parsed.root}`);
              break;

            case 'CHUNK_PROOF':
              await this.handleChunkProof(providerId, parsed);
              break;

            case 'CHUNK_DATA':
              // Provider is sending us chunk data in response to a read request
              await this.handleChunkData(providerId, parsed);
              break;

            default:
              logger.warn(`Unknown message type from ${providerId}: ${parsed.type}`);
          }
        } catch (e) {
          logger.warn(`Failed to parse message from ${providerId}: ${data}`, e);
        }
      });

      ws.on('close', () => {
        provider.isOnline = false;
        logger.info(`Provider offline => ${providerId}`);
      });
    });

    return new Promise<void>((resolve) => {
      server.listen(this.bootPort, () => {
        logger.info(`WebSocket server (and HTTP) listening on port ${this.bootPort}`);
        resolve();
      });
    });
  }

  /**
   * Start the REST API server
   */
  private async startApiServer() {
    const app = express();
    app.use(cors());
    app.use(express.json({ limit: '50mb' }));

    // Multer => for any file input
    const storage = multer.memoryStorage();
    const upload = multer({ storage });

    /**
     * POST /upload => handle file or text input
     * No chunks are stored on the Host; we push them to Providers
     */
    app.post('/upload', upload.any(), async (req, res) => {
      try {
        if (req.files && Array.isArray(req.files) && req.files.length > 0) {
          // Multiple file(s)
          const results = [];
          for (const file of req.files as Express.Multer.File[]) {
            const fileBuffer = file.buffer;
            const fileName = file.originalname || 'unknown';
            const contentType = file.mimetype || 'application/octet-stream';
            const fileSize = file.size || fileBuffer.length;

            // 1) Generate random hash
            const fileHash = generateRandomHash();

            // 2) Split into 64KB chunks in-memory
            const chunkSize = 64 * 1024;
            const chunks = chunkBuffer(fileBuffer, chunkSize);

            // 3) Hash each chunk, send to Providers
            const chunkHashes: string[] = [];
            for (const chunk of chunks) {
              const cHash = generateHash(chunk);
              chunkHashes.push(cHash);
              // Instruct providers to store it now
              this.instructProvidersToStoreChunk(cHash, chunk);
            }

            // 4) Build Merkle root
            const { root: merkleRoot } = buildMerkleTree(chunkHashes);

            // 5) Store metadata in Host
            const fileMeta: FileMeta = {
              fileHash,
              merkleRoot,
              chunkHashes,
              chunkProviders: {}, // you can fill if you want
              fileName,
              contentType,
              fileSize,
            };
            await this.hashStore.storeFileMeta(fileHash, fileMeta);
            await this.proofStore.storeProof(fileHash, { merkleRoot });

            results.push({ fileHash, merkleRoot, fileName, contentType, fileSize });
          }

          return res.json({ uploaded: results });
        } else if (req.body && req.body.data) {
          // Plain text
          const textBuffer = Buffer.from(req.body.data);

          const fileHash = generateRandomHash();
          const chunkSize = 64 * 1024;
          const chunks = chunkBuffer(textBuffer, chunkSize);

          const chunkHashes: string[] = [];
          for (const chunk of chunks) {
            const cHash = generateHash(chunk);
            chunkHashes.push(cHash);
            this.instructProvidersToStoreChunk(cHash, chunk);
          }

          const { root: merkleRoot } = buildMerkleTree(chunkHashes);

          const fileMeta: FileMeta = {
            fileHash,
            merkleRoot,
            chunkHashes,
            chunkProviders: {},
            fileName: 'plaintext.txt',
            contentType: 'text/plain',
            fileSize: textBuffer.length,
          };
          await this.hashStore.storeFileMeta(fileHash, fileMeta);
          await this.proofStore.storeProof(fileHash, { merkleRoot });

          return res.json({
            fileHash,
            merkleRoot,
            fileName: fileMeta.fileName,
            contentType: fileMeta.contentType,
            fileSize: fileMeta.fileSize,
          });
        } else {
          return res.status(400).json({ error: 'No file or data provided.' });
        }
      } catch (err: any) {
        logger.error('Error in /upload:', err);
        return res.status(500).json({ error: err.message });
      }
    });

    /**
     * GET /file/:fileHash => fetch chunk data from Providers, inline or download
     */
    app.get('/file/:fileHash', async (req, res) => {
      try {
        const { fileHash } = req.params;
        const meta = await this.hashStore.retrieveFileMeta(fileHash);
        if (!meta) {
          return res.status(404).send('File not found in metadata.');
        }

        // We'll retrieve chunk data from the Providers by sending a message
        // Then wait until all chunk data is returned
        const chunkHashes = meta.chunkHashes;
        if (!chunkHashes || chunkHashes.length === 0) {
          return res.status(500).send('No chunks associated with this file.');
        }

        // Create an in-memory promise that waits for chunk data
        const fileBuf = await this.requestAllChunks(fileHash, chunkHashes);

        // Decide whether inline or download
        const contentType = (meta as any).contentType || 'application/octet-stream';
        const fileName = (meta as any).fileName || fileHash;

        if (INLINE_MIME_TYPES.has(contentType)) {
          res.setHeader('Content-Type', contentType);
          res.send(fileBuf);
        } else {
          res.setHeader('Content-Type', 'application/octet-stream');
          res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
          res.send(fileBuf);
        }
      } catch (err: any) {
        logger.error('Error in /file:', err);
        return res.status(500).send(err.message);
      }
    });

    /**
     * Example: manual challenge
     */
    app.get('/challenge/:providerId/:chunkHash', (req, res) => {
      const { providerId, chunkHash } = req.params;
      const provider = this.providers.get(providerId);
      if (!provider || !provider.isOnline) {
        return res.status(404).json({ error: 'Provider not found or offline.' });
      }

      provider.ws.send(
        JSON.stringify({
          type: 'REQUEST_CHUNK_PROOF',
          chunkHash,
        })
      );

      return res.json({ message: `Challenge sent to ${providerId} for chunk ${chunkHash}` });
    });

    return new Promise<void>((resolve) => {
      app.listen(this.apiPort, () => {
        logger.info(`API server listening on port ${this.apiPort}`);
        resolve();
      });
    });
  }

  /**
   * Send chunk data to providers (no local storage)
   */
  private instructProvidersToStoreChunk(chunkHash: string, chunkData: Buffer) {
    // For simplicity, we send the chunk to all online providers
    // Or pick a subset if you want
    const onlineProviders = Array.from(this.providers.values()).filter((p) => p.isOnline);

    // In real logic, you might pick only 2 or 3
    onlineProviders.forEach((provider) => {
      provider.assignedChunks.add(chunkHash);
      provider.ws.send(
        JSON.stringify({
          type: 'STORE_CHUNK',
          chunkHash,
          data: chunkData.toString('hex'),
        })
      );
    });
  }

  /**
   * Request chunk data from providers for reading
   * Returns a single Buffer (all chunks concatenated)
   */
  private requestAllChunks(fileHash: string, chunkHashes: string[]): Promise<Buffer> {
    return new Promise<Buffer>((resolve, reject) => {
      // create a readRequest object
      const readReq: ReadRequest = {
        totalChunks: chunkHashes.length,
        chunksReceived: 0,
        chunkData: {},
        resolve,
        reject,
      };

      // store it so we can track chunk arrivals
      this.readRequests.set(fileHash, readReq);

      // broadcast chunk requests
      const onlineProviders = Array.from(this.providers.values()).filter((p) => p.isOnline);
      if (onlineProviders.length === 0) {
        return reject(new Error('No providers online to retrieve chunks.'));
      }

      for (const ch of chunkHashes) {
        // we ask all providers for each chunk, but presumably only one has it
        // or multiple might, but as soon as we get it once, we can proceed
        for (const provider of onlineProviders) {
          provider.ws.send(
            JSON.stringify({
              type: 'REQUEST_CHUNK_DATA',
              fileHash,
              chunkHash: ch,
            })
          );
        }
      }
    });
  }

  /**
   * Handle CHUNK_DATA from a Provider
   * We'll see if the read request is in progress, store the chunk, and check if done
   */
  private async handleChunkData(providerId: string, msg: any) {
    const { fileHash, chunkHash, dataHex } = msg;

    // Check if there's a read request pending for this file
    const readReq = this.readRequests.get(fileHash);
    if (!readReq) {
      logger.warn(`Received CHUNK_DATA for fileHash=${fileHash}, but no read request in progress`);
      return;
    }

    // If we already have that chunk, skip
    if (readReq.chunkData[chunkHash]) {
      return;
    }

    const buf = Buffer.from(dataHex, 'hex');
    readReq.chunkData[chunkHash] = buf;
    readReq.chunksReceived++;

    // Check if we've got all chunks
    if (readReq.chunksReceived === readReq.totalChunks) {
      // remove from map
      this.readRequests.delete(fileHash);

      // sort chunkData by the order in chunkHashes? 
      // We didn't store the original order, so let's assume 
      // the original logic is that chunkHashes are in the right order
      const meta = await this.hashStore.retrieveFileMeta(fileHash);
      if (!meta) {
        readReq.reject(new Error('File metadata missing.'));
        return;
      }

      const buffers: Buffer[] = [];
      for (const ch of meta.chunkHashes) {
        const cbuf = readReq.chunkData[ch];
        if (!cbuf) {
          readReq.reject(new Error(`Missing chunk data: ${ch}`));
          return;
        }
        buffers.push(cbuf);
      }

      const fileBuf = Buffer.concat(buffers);
      readReq.resolve(fileBuf);
    }
  }

  /**
   * Handle CHUNK_PROOF message (unchanged logic)
   */
  private async handleChunkProof(providerId: string, msg: any) {
    const { chunkHash, proof, merkleRoot, encryptedHex } = msg;
    logger.info(`CHUNK_PROOF from ${providerId} for chunk=${chunkHash} root=${merkleRoot}`);

    await this.proofStore.storeProof(`${providerId}-${chunkHash}`, {
      chunkHash,
      proof,
      merkleRoot,
      encryptedHex,
      timestamp: Date.now(),
    });
    logger.info(`Stored chunk proof => ${providerId}-${chunkHash}`);
  }

  /**
   * Random chunk challenge
   * (But we don't store anything locally, so we might not know about chunk hashes unless we track them in file metadata)
   */
  private async issueRandomChunkChallenge() {
    try {
      // For demonstration, let's pick a random file from hashStore, then a random chunk
      const allFiles = await this.hashStore.getAllFileMetaKeys();
      if (allFiles.length === 0) {
        logger.info('No files in metadata, skipping random challenge.');
        return;
      }
      const randomFile = allFiles[Math.floor(Math.random() * allFiles.length)];
      const meta = await this.hashStore.retrieveFileMeta(randomFile);
      if (!meta || !meta.chunkHashes || meta.chunkHashes.length === 0) {
        logger.info(`File ${randomFile} has no chunks, skipping challenge.`);
        return;
      }
      const randomChunk = meta.chunkHashes[Math.floor(Math.random() * meta.chunkHashes.length)];

      // pick an online provider
      const onlineProviders = Array.from(this.providers.values()).filter((p) => p.isOnline);
      if (onlineProviders.length === 0) {
        logger.info('No online providers to challenge.');
        return;
      }
      const chosen = onlineProviders[Math.floor(Math.random() * onlineProviders.length)];

      logger.info(`Issuing random challenge => ${chosen.id} for chunk=${randomChunk}`);
      chosen.ws.send(
        JSON.stringify({
          type: 'REQUEST_CHUNK_PROOF',
          chunkHash: randomChunk,
        })
      );
    } catch (err) {
      logger.error('Error in issueRandomChunkChallenge:', err);
    }
  }
}
