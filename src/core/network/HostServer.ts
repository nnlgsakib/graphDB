import express from 'express';
import http from 'http';
import WebSocket from 'ws';
import multer from 'multer';
import logger from '../utils/logger';
import { DataStore } from '../storage/DataStore';
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

export class HostServer {
  private bootPort: number;
  private apiPort: number;

  private dataStore: DataStore;
  private hashStore: HashStore;
  private proofStore: ProofStore;

  private providers: Map<string, ProviderInfo> = new Map();
  private providerCounter = 0;
  private challengeIntervalMs = 5 * 60 * 1000;
  private challengeInterval: NodeJS.Timeout | null = null;

  constructor(bootPort: number, apiPort: number, dbDir: string) {
    this.bootPort = bootPort;
    this.apiPort = apiPort;

    this.dataStore = new DataStore(`${dbDir}/data`);
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
   * Spin up WebSocket server (on top of an HTTP server) for Provider connections
   */
  private async startWebSocketServer() {
    const server = http.createServer();

    // Attach a WebSocket.Server to that HTTP server
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
   * Spin up REST API server
   */
  private async startApiServer() {
    const app = express();
    app.use(express.json({ limit: '50mb' }));

    // Multer setup => memoryStorage, so we can handle file(s) in req.files
    const storage = multer.memoryStorage();
    const upload = multer({ storage });

    /**
     * POST /upload => handle:
     *   - Single or multiple files
     *   - Plaintext in req.body.data if no file is provided
     */
    app.post('/upload', upload.any(), async (req, res) => {
        try {
          // Safely check if req.files is an array and has length > 0
          if (req.files && Array.isArray(req.files) && req.files.length > 0) {
            const results = [];
      
            // Process each file
            for (const file of req.files as Express.Multer.File[]) {
              const fileBuffer = file.buffer;
              const fileName = file.originalname || 'unknown';
              const contentType = file.mimetype || 'application/octet-stream';
              const fileSize = file.size || fileBuffer.length;
      
              // 1) Generate random hash for the file
              const fileHash = generateRandomHash();
      
              // 2) Split into 64KB chunks
              const chunkSize = 64 * 1024;
              const chunks = chunkBuffer(fileBuffer, chunkSize);
      
              const chunkHashes: string[] = [];
              for (const chunk of chunks) {
                const cHash = generateHash(chunk);
                await this.dataStore.storeData(cHash, chunk);
                chunkHashes.push(cHash);
              }
      
              // 3) Build Merkle root
              const { root: merkleRoot } = buildMerkleTree(chunkHashes);
      
              // 4) Replicate => pick 2 providers
              const chunkProviders = this.replicateChunks(chunkHashes);
              for (const cHash of chunkHashes) {
                const dataBuf = await this.dataStore.retrieveData(cHash);
                if (!dataBuf) continue;
                await this.instructProvidersToStoreChunk(cHash, dataBuf, chunkProviders[cHash]);
              }
      
              // 5) Store extended metadata => fileName, contentType, fileSize
              const fileMeta: FileMeta = {
                fileHash,
                merkleRoot,
                chunkHashes,
                chunkProviders,
                fileName,
                contentType,
                fileSize,
              } as FileMeta;
      
              await this.hashStore.storeFileMeta(fileHash, fileMeta);
              await this.proofStore.storeProof(fileHash, { merkleRoot });
      
              results.push({
                fileHash,
                merkleRoot,
                fileName,
                contentType,
                fileSize,
              });
            }
      
            // If multiple files were uploaded, return them all. Otherwise, array with one
            return res.json({ uploaded: results });
          }
          // Otherwise, check for plain text in req.body.data
          else if (req.body && req.body.data) {
            // Convert plain text to a Buffer
            const textBuffer = Buffer.from(req.body.data);
      
            // 1) Generate random hash for the "file"
            const fileHash = generateRandomHash();
      
            // 2) Split into 64KB chunks
            const chunkSize = 64 * 1024;
            const chunks = chunkBuffer(textBuffer, chunkSize);
      
            const chunkHashes: string[] = [];
            for (const chunk of chunks) {
              const cHash = generateHash(chunk);
              await this.dataStore.storeData(cHash, chunk);
              chunkHashes.push(cHash);
            }
      
            // 3) Build Merkle root
            const { root: merkleRoot } = buildMerkleTree(chunkHashes);
      
            // 4) Replicate => pick 2 providers
            const chunkProviders = this.replicateChunks(chunkHashes);
            for (const cHash of chunkHashes) {
              const dataBuf = await this.dataStore.retrieveData(cHash);
              if (!dataBuf) continue;
              await this.instructProvidersToStoreChunk(cHash, dataBuf, chunkProviders[cHash]);
            }
      
            // 5) Store metadata (placeholder name, etc.)
            const fileMeta: FileMeta = {
              fileHash,
              merkleRoot,
              chunkHashes,
              chunkProviders,
              fileName: 'plaintext.txt',
              contentType: 'text/plain',
              fileSize: textBuffer.length,
            } as FileMeta;
      
            await this.hashStore.storeFileMeta(fileHash, fileMeta);
            await this.proofStore.storeProof(fileHash, { merkleRoot });
      
            return res.json({
              fileHash,
              merkleRoot,
              fileName: fileMeta.fileName,
              contentType: fileMeta.contentType,
              fileSize: fileMeta.fileSize,
            });
          }
          // Neither files nor plaintext
          else {
            return res.status(400).json({ error: 'No file or data provided.' });
          }
        } catch (err: any) {
          logger.error('Error in /upload:', err);
          return res.status(500).json({ error: err.message });
        }
      });
      

    /**
     * GET /file/:fileHash => inline render if possible, else download
     */
    app.get('/file/:fileHash', async (req, res) => {
      try {
        const { fileHash } = req.params;
        const meta = await this.hashStore.retrieveFileMeta(fileHash);
        if (!meta) {
          return res.status(404).send('File not found in metadata.');
        }

        // Reassemble data from raw chunks
        const chunkDataList: Buffer[] = [];
        for (const cHash of meta.chunkHashes) {
          const buf = await this.dataStore.retrieveData(cHash);
          if (!buf) {
            return res.status(500).send(`Missing chunk: ${cHash}`);
          }
          chunkDataList.push(buf);
        }
        const fileBuf = Buffer.concat(chunkDataList);

        // Content type + filename from metadata (or fallback)
        const contentType = (meta as any).contentType || 'application/octet-stream';
        const fileName = (meta as any).fileName || fileHash;

        // Decide whether to render inline or force download
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
     * Example: Manual challenge request
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

    // Start the API server
    return new Promise<void>((resolve) => {
      app.listen(this.apiPort, () => {
        logger.info(`API server listening on port ${this.apiPort}`);
        resolve();
      });
    });
  }

  /**
   * Handle CHUNK_PROOF message from provider
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
   * Distribute chunk storage among providers
   */
  private replicateChunks(chunkHashes: string[]): Record<string, string[]> {
    const result: Record<string, string[]> = {};
    const onlineProviders = Array.from(this.providers.values()).filter((p) => p.isOnline);

    if (onlineProviders.length === 0) {
      // If no providers, everything is host-only
      chunkHashes.forEach((ch) => {
        result[ch] = ['host-only'];
      });
      return result;
    }

    let idx = 0;
    for (const ch of chunkHashes) {
      const assigned: string[] = [];
      for (let i = 0; i < 2; i++) {
        const prov = onlineProviders[idx % onlineProviders.length];
        assigned.push(prov.id);
        idx++;
      }
      result[ch] = assigned;
    }
    return result;
  }

  /**
   * Instruct providers to store chunk data
   */
  private async instructProvidersToStoreChunk(
    chunkHash: string,
    rawData: Buffer,
    providerIds: string[],
  ) {
    for (const pid of providerIds) {
      if (pid === 'host-only') continue;
      const provider = this.providers.get(pid);
      if (!provider || !provider.isOnline) continue;

      provider.assignedChunks.add(chunkHash);

      provider.ws.send(
        JSON.stringify({
          type: 'STORE_CHUNK',
          chunkHash,
          data: rawData.toString('hex'),
        })
      );
    }
  }

  /**
   * Periodically pick a random chunk and provider for a challenge
   */
  private async issueRandomChunkChallenge() {
    try {
      const allChunks = await this.dataStore.getAllKeys();
      if (allChunks.length === 0) {
        logger.info('No chunks stored, skipping random challenge.');
        return;
      }
      const randomChunk = allChunks[Math.floor(Math.random() * allChunks.length)];

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
