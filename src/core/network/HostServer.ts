import express from 'express';
import http from 'http';
import cors from 'cors';
import WebSocket from 'ws';
import multer from 'multer';
import logger from '../utils/logger';
import { generateHash, generateRandomHash, chunkBuffer } from '../utils/cryptoutils';
import { buildMerkleTree } from '../utils/merkleutils';

interface ProviderInfo {
  id: string;
  ws: WebSocket;
  isOnline: boolean;
  merkleRoot: string | null;
  storedHashes: Set<string>;
  metadata: Map<string, FileMeta>;
}

interface FileMeta {
  fileHash: string;
  merkleRoot: string;
  chunkHashes: string[];
  fileName: string;
  contentType: string;
  fileSize: number;
  timestamp: number;
  providerIds: string[];
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
  private metadataCache: Map<string, FileMeta> = new Map();

  constructor(bootPort: number, apiPort: number) {
    this.bootPort = bootPort;
    this.apiPort = apiPort;
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
    const failedProvider = this.providers.get(failedProviderId);
    if (!failedProvider) return;

    const onlineProviders = Array.from(this.providers.values())
      .filter(p => p.isOnline && p.id !== failedProviderId);
    
    if (onlineProviders.length < this.minProviderCount) {
      logger.error('Not enough providers for redistribution');
      return;
    }

    failedProvider.metadata.forEach((meta, fileHash) => {
      const targetProviders = this.selectProviders(onlineProviders, this.replicationFactor);
      targetProviders.forEach(provider => {
        provider.metadata.set(fileHash, meta);
        provider.ws.send(JSON.stringify({
          type: 'SYNC_METADATA',
          fileHash,
          metadata: meta
        }));
      });
    });
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
        storedHashes: new Set(),
        metadata: new Map()
      };
      
      this.providers.set(providerId, provider);
      logger.info(`Provider connected => ${providerId}`);

      this.syncProviderState(provider);

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
        
        if (INLINE_MIME_TYPES.has(metadata.contentType)) {
          res.setHeader('Content-Type', metadata.contentType);
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
        const files = Array.from(this.metadataCache.values())
          .map(({ fileHash, fileName, contentType, fileSize, timestamp }) => ({
            fileHash, fileName, contentType, fileSize, timestamp
          }));
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
      fileName: file.originalname,
      contentType: file.mimetype,
      fileSize: file.size,
      timestamp: Date.now(),
      providerIds: []
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
      fileName: 'text.txt',
      contentType: 'text/plain',
      fileSize: buffer.length,
      timestamp: Date.now(),
      providerIds: []
    };

    await this.distributeChunks(chunks, chunkHashes, metadata);
    return metadata;
  }

  private async distributeChunks(chunks: Buffer[], chunkHashes: string[], metadata: FileMeta) {
    const onlineProviders = Array.from(this.providers.values())
      .filter(p => p.isOnline);
    
    const targetProviders = this.selectProviders(onlineProviders, this.replicationFactor);
    metadata.providerIds = targetProviders.map(p => p.id);

    for (let i = 0; i < chunks.length; i++) {
      const chunk = chunks[i];
      const chunkHash = chunkHashes[i];

      targetProviders.forEach(provider => {
        provider.storedHashes.add(chunkHash);
        provider.metadata.set(metadata.fileHash, metadata);
        
        provider.ws.send(JSON.stringify({
          type: 'STORE_CHUNK',
          chunkHash,
          data: chunk.toString('hex'),
          metadata
        }));
      });
    }

    this.metadataCache.set(metadata.fileHash, metadata);
  }

  private async getFile(fileHash: string) {
    const metadata = this.metadataCache.get(fileHash);
    if (!metadata) return null;

    const buffer = await this.assembleFile(fileHash, metadata.chunkHashes);
    return { buffer, metadata };
  }

  private assembleFile(fileHash: string, chunkHashes: string[]): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      const readReq: ReadRequest = {
        totalChunks: chunkHashes.length,
        chunksReceived: 0,
        chunkData: {},
        resolve,
        reject
      };

      this.readRequests.set(fileHash, readReq);

      const onlineProviders = Array.from(this.providers.values())
        .filter(p => p.isOnline);

      if (onlineProviders.length === 0) {
        reject(new Error('No providers available'));
        return;
      }

      chunkHashes.forEach(chunkHash => {
        onlineProviders.forEach(provider => {
          if (provider.storedHashes.has(chunkHash)) {
            provider.ws.send(JSON.stringify({
              type: 'REQUEST_CHUNK_DATA',
              fileHash,
              chunkHash
            }));
          }
        });
      });
    });
  }

  private async handleProviderMessage(providerId: string, message: any) {
    const provider = this.providers.get(providerId);
    if (!provider) return;

    switch (message.type) {
      case 'SYNC_STATE':
        provider.metadata = new Map(Object.entries(message.metadata));
        provider.storedHashes = new Set(message.storedHashes);
        provider.merkleRoot = message.merkleRoot;
        this.updateMetadataCache(provider.metadata);
        break;

      case 'METADATA_UPDATE':
        provider.metadata.set(message.fileHash, message.metadata);
        this.metadataCache.set(message.fileHash, message.metadata);
        this.broadcastToProviders({
          type: 'SYNC_METADATA',
          fileHash: message.fileHash,
          metadata: message.metadata
        }, [providerId]);
        break;

      case 'CHUNK_DATA':
        await this.handleChunkData(message);
        break;
    }
  }

  private updateMetadataCache(providerMetadata: Map<string, FileMeta>) {
    providerMetadata.forEach((meta, fileHash) => {
      if (!this.metadataCache.has(fileHash)) {
        this.metadataCache.set(fileHash, meta);
      }
    });
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
        const metadata = this.metadataCache.get(fileHash);
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

  private broadcastToProviders(message: any, excludeIds: string[] = []) {
    this.providers.forEach((provider, id) => {
      if (provider.isOnline && !excludeIds.includes(id)) {
        provider.ws.send(JSON.stringify(message));
      }
    });
  }

  private async syncProviderState(newProvider: ProviderInfo) {
    const activeProvider = Array.from(this.providers.values())
      .find(p => p.isOnline && p.id !== newProvider.id);
    
    if (activeProvider) {
      activeProvider.ws.send(JSON.stringify({
        type: 'REQUEST_SYNC',
        targetProviderId: newProvider.id
      }));
    }
  }
}