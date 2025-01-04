import express from 'express';
import http from 'http';
import cors from 'cors';
import WebSocket from 'ws';
import multer from 'multer';
import logger from '../utils/logger';
import { generateHash, generateRandomHash, chunkBuffer } from '../utils/cryptoutils';
import { buildMerkleTree } from '../utils/merkleutils';
import { HashStore, FileMeta } from './../storage/HashStore';

interface ProviderInfo {
  id: string;
  ws: WebSocket;
  isOnline: boolean;
  merkleRoot: string | null;
  storedChunks: Set<string>;
}

interface ReadRequest {
  totalChunks: number;
  chunksReceived: number;
  chunkData: Record<string, Buffer>;
  resolve: (buf: Buffer) => void;
  reject: (err: Error) => void;
}

const INLINE_MIME_TYPES = new Set([
  'image/png', 'image/jpeg', 'image/gif', 'image/webp',
  'text/html', 'text/plain', 'application/pdf'
]);

export class HostServer {
  private bootPort: number;
  private apiPort: number;
  private providers: Map<string, ProviderInfo> = new Map();
  private providerCounter = 0;
  private readRequests: Map<string, ReadRequest> = new Map();
  private minProviderCount = 3;
  private replicationFactor = 3;
  private healthCheckInterval: NodeJS.Timeout | null = null;
  private hashStore: HashStore;

  constructor(bootPort: number, apiPort: number, storagePath: string) {
    this.bootPort = bootPort;
    this.apiPort = apiPort;
    this.hashStore = new HashStore(`${storagePath}/hashes`);
  }

  public async start() {
    await this.startWebSocketServer();
    await this.startApiServer();
    this.startHealthCheck();
    logger.info(`HostServer up => bootPort=${this.bootPort}, apiPort=${this.apiPort}`);
  }

  private startHealthCheck() {
    this.healthCheckInterval = setInterval(() => {
      this.providers.forEach((provider, id) => {
        if (provider.isOnline) {
          provider.ws.ping();
          const timeout = setTimeout(() => {
            provider.isOnline = false;
            this.redistributeProviderData(id);
          }, 5000);

          provider.ws.once('pong', () => {
            clearTimeout(timeout);
          });
        }
      });
    }, 30000);
  }

  private async redistributeProviderData(failedProviderId: string) {
    const onlineProviders = Array.from(this.providers.values())
      .filter(p => p.isOnline && p.id !== failedProviderId);
    
    if (onlineProviders.length < this.minProviderCount) {
      logger.error('Not enough providers for redistribution');
      return;
    }

    const allHashes = await this.hashStore.getAllFileMetaKeys();
    for (const fileHash of allHashes) {
      const meta = await this.hashStore.retrieveFileMeta(fileHash);
      if (!meta) continue;

      Object.keys(meta.chunkProviders).forEach(chunkHash => {
        meta.chunkProviders[chunkHash] = meta.chunkProviders[chunkHash]
          .filter(id => id !== failedProviderId);
      });

      const targetProviders = this.selectProviders(onlineProviders, this.replicationFactor);
      Object.keys(meta.chunkProviders).forEach(chunkHash => {
        const currentProviders = meta.chunkProviders[chunkHash];
        const neededProviders = this.replicationFactor - currentProviders.length;
        
        if (neededProviders > 0) {
          const newProviders = targetProviders
            .filter(p => !currentProviders.includes(p.id))
            .slice(0, neededProviders)
            .map(p => p.id);

          meta.chunkProviders[chunkHash].push(...newProviders);

          // Request chunk redistribution from existing providers
          const existingProvider = this.providers.get(currentProviders[0]);
          if (existingProvider) {
            existingProvider.ws.send(JSON.stringify({
              type: 'REDISTRIBUTE_CHUNK',
              chunkHash,
              targetProviders: newProviders
            }));
          }
        }
      });

      await this.hashStore.storeFileMeta(fileHash, meta);
    }
  }

  private selectProviders(providers: ProviderInfo[], count: number): ProviderInfo[] {
    return providers
      .sort(() => Math.random() - 0.5)
      .slice(0, Math.min(count, providers.length));
  }

  private async startWebSocketServer() {
    const server = http.createServer();
    const wss = new WebSocket.Server({ server });

    wss.on('connection', (ws) => {
      const providerId = `provider-${++this.providerCounter}`;
      const provider: ProviderInfo = {
        id: providerId,
        ws,
        isOnline: true,
        merkleRoot: null,
        storedChunks: new Set()
      };
      
      this.providers.set(providerId, provider);
      logger.info(`Provider connected => ${providerId}`);

      ws.on('message', async (message) => {
        try {
          const data = JSON.parse(message.toString());
          await this.handleProviderMessage(providerId, data);
        } catch (error) {
          logger.error('Error handling provider message:', error);
        }
      });

      ws.on('close', () => {
        provider.isOnline = false;
        this.redistributeProviderData(providerId);
      });

      ws.on('error', (error) => {
        logger.error(`WebSocket error for provider ${providerId}:`, error);
        provider.isOnline = false;
      });
    });

    return new Promise<void>((resolve) => {
      server.listen(this.bootPort, () => resolve());
    });
  }

  private async startApiServer() {
    const app = express();
    app.use(cors());
    app.use(express.json({ limit: '50mb' }));
    
    const storage = multer.memoryStorage();
    const upload = multer({ storage });

    app.post('/upload', upload.any(), async (req, res) => {
      try {
        if (!this.hasEnoughProviders()) {
          return res.status(503).json({ error: 'Not enough providers available' });
        }

        const results = await this.handleUpload(req);
        res.json({ uploaded: results });
      } catch (error: any) {
        logger.error('Upload error:', error);
        res.status(500).json({ error: error.message });
      }
    });

    app.get('/file/:fileHash', async (req, res) => {
      try {
        const { fileHash } = req.params;
        const file = await this.getFile(fileHash);
        
        if (!file) {
          return res.status(404).json({ error: 'File not found' });
        }

        const { buffer, metadata } = file;
        
        if (INLINE_MIME_TYPES.has(metadata.contentType!)) {
          res.setHeader('Content-Type', metadata.contentType!);
          res.send(buffer);
        } else {
          res.setHeader('Content-Type', 'application/octet-stream');
          res.setHeader('Content-Disposition', `attachment; filename="${metadata.fileName}"`);
          res.send(buffer);
        }
      } catch (error: any) {
        logger.error('File retrieval error:', error);
        res.status(500).json({ error: error.message });
      }
    });

    app.get('/files', async (req, res) => {
      try {
        const fileHashes = await this.hashStore.getAllFileMetaKeys();
        const files = await Promise.all(
          fileHashes.map(async hash => {
            const meta = await this.hashStore.retrieveFileMeta(hash);
            return {
              fileHash: hash,
              fileName: meta?.fileName,
              contentType: meta?.contentType,
              fileSize: meta?.fileSize
            };
          })
        );
        res.json({ files });
      } catch (error: any) {
        res.status(500).json({ error: error.message });
      }
    });

    return new Promise<void>((resolve) => {
      app.listen(this.apiPort, () => resolve());
    });
  }

  private hasEnoughProviders(): boolean {
    const onlineCount = Array.from(this.providers.values())
      .filter(p => p.isOnline).length;
    return onlineCount >= this.minProviderCount;
  }

  private async handleUpload(req: express.Request) {
    const results = [];
    
    if (req.files && Array.isArray(req.files)) {
      for (const file of req.files as Express.Multer.File[]) {
        const result = await this.processFile(file);
        results.push(result);
      }
    } else if (req.body && req.body.data) {
      const result = await this.processTextData(req.body.data);
      results.push(result);
    }
    
    return results;
  }

  private async processFile(file: Express.Multer.File) {
    const fileHash = generateRandomHash();
    const chunks = chunkBuffer(file.buffer, 64 * 1024);
    const chunkHashes = chunks.map(chunk => generateHash(chunk));
    
    const { root: merkleRoot } = buildMerkleTree(chunkHashes);
    
    const metadata: FileMeta = {
      fileHash,
      merkleRoot,
      chunkHashes,
      chunkProviders: {},
      fileName: file.originalname,
      contentType: file.mimetype,
      fileSize: file.size
    };

    await this.distributeChunks(chunks, chunkHashes, metadata);
    return metadata;
  }

  private async processTextData(text: string) {
    const buffer = Buffer.from(text);
    const fileHash = generateRandomHash();
    const chunks = chunkBuffer(buffer, 64 * 1024);
    const chunkHashes = chunks.map(chunk => generateHash(chunk));
    
    const { root: merkleRoot } = buildMerkleTree(chunkHashes);
    
    const metadata: FileMeta = {
      fileHash,
      merkleRoot,
      chunkHashes,
      chunkProviders: {},
      fileName: 'text.txt',
      contentType: 'text/plain',
      fileSize: buffer.length
    };

    await this.distributeChunks(chunks, chunkHashes, metadata);
    return metadata;
  }

  private async distributeChunks(chunks: Buffer[], chunkHashes: string[], metadata: FileMeta) {
    const onlineProviders = Array.from(this.providers.values())
      .filter(p => p.isOnline);
    
    const targetProviders = this.selectProviders(onlineProviders, this.replicationFactor);
    const providerIds = targetProviders.map(p => p.id);

    for (let i = 0; i < chunks.length; i++) {
      const chunk = chunks[i];
      const chunkHash = chunkHashes[i];

      metadata.chunkProviders[chunkHash] = providerIds;

      targetProviders.forEach(provider => {
        provider.storedChunks.add(chunkHash);
        provider.ws.send(JSON.stringify({
          type: 'STORE_CHUNK',
          chunkHash,
          data: chunk.toString('hex'),
          metadata
        }));
      });
    }

    await this.hashStore.storeFileMeta(metadata.fileHash, metadata);
  }

  private async getFile(fileHash: string) {
    const metadata = await this.hashStore.retrieveFileMeta(fileHash);
    if (!metadata) return null;

    const buffer = await this.assembleFile(fileHash, metadata);
    return { buffer, metadata };
  }

  private assembleFile(fileHash: string, metadata: FileMeta): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      const readReq: ReadRequest = {
        totalChunks: metadata.chunkHashes.length,
        chunksReceived: 0,
        chunkData: {},
        resolve,
        reject
      };

      this.readRequests.set(fileHash, readReq);

      metadata.chunkHashes.forEach((chunkHash) => {
        const providers = metadata.chunkProviders[chunkHash] || [];
        const availableProvider = providers
          .map(id => this.providers.get(id))
          .find(p => p && p.isOnline && p.storedChunks.has(chunkHash));

        if (availableProvider) {
          availableProvider.ws.send(JSON.stringify({
            type: 'REQUEST_CHUNK',
            fileHash,
            chunkHash
          }));
        } else {
          readReq.reject(new Error(`No available provider for chunk ${chunkHash}`));
        }
      });
    });
  }

  private async handleProviderMessage(providerId: string, message: any) {
    const provider = this.providers.get(providerId);
    if (!provider) return;

    switch (message.type) {
      case 'CHUNK_DATA':
        await this.handleChunkData(message);
        break;

      case 'MERKLE_ROOT':
        provider.merkleRoot = message.root;
        break;

      case 'SYNC_STATE':
        provider.storedChunks = new Set(message.storedChunks);
        break;
    }
  }

  private async handleChunkData(message: any) {
    const { fileHash, chunkHash, data } = message;
    const readReq = this.readRequests.get(fileHash);
    if (!readReq) return;

    if (!readReq.chunkData[chunkHash]) {
      const chunk = Buffer.from(data, 'hex');
      readReq.chunkData[chunkHash] = chunk;
      readReq.chunksReceived++;

      if (readReq.chunksReceived === readReq.totalChunks) {
        const metadata = await this.hashStore.retrieveFileMeta(fileHash);
        if (!metadata) {
          readReq.reject(new Error('File metadata missing'));
          return;
        }

        const buffers = metadata.chunkHashes
          .map(hash => readReq.chunkData[hash]);
        
        if (buffers.some(b => !b)) {
          readReq.reject(new Error('Missing chunks'));
          return;
        }

        const fileBuffer = Buffer.concat(buffers);
        this.readRequests.delete(fileHash);
        readReq.resolve(fileBuffer);
      }
    }
  }
}