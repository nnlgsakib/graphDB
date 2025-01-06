
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
  name: string;  // Added name field
  ws: WebSocket;
  isOnline: boolean;
  merkleRoot: string | null;
  storedChunks: Set<string>;
  knownMetadata: Set<string>; // Track which file metadata this provider knows about
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
  private generateProviderId(providedName?: string): string {
    if (providedName) {
      return providedName;
    }
    // Generate a short SHA-256 hash (first 8 characters)
    return generateRandomHash().substring(0, 8);
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
    const loggedRedistributions = new Set<string>();
  
    for (const fileHash of allHashes) {
      const meta = await this.hashStore.retrieveFileMeta(fileHash);
      if (!meta) continue;
  
      Object.keys(meta.chunkProviders).forEach(chunkHash => {
        if (!loggedRedistributions.has(`${chunkHash}-${failedProviderId}`)) {
          meta.chunkProviders[chunkHash] = meta.chunkProviders[chunkHash]
            .filter(id => id !== failedProviderId);
  
          const targetProviders = this.selectProviders(onlineProviders, this.replicationFactor);
          const newProviders = targetProviders.map(p => p.id);
  
          logger.info(`Redistributing chunk ${chunkHash} from failed provider ${failedProviderId} to providers: ${newProviders.join(', ')}`);
          loggedRedistributions.add(`${chunkHash}-${failedProviderId}`);
  
          const existingProvider = this.providers.get(meta.chunkProviders[chunkHash][0]);
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
      ws.once('message', async (message) => {
        try {
          const initialData = JSON.parse(message.toString());
          const providerId = this.generateProviderId(initialData.providerName);
          
          const provider: ProviderInfo = {
            id: providerId,
            name: initialData.providerName || providerId,
            ws,
            isOnline: true,
            merkleRoot: null,
            storedChunks: new Set(),
            knownMetadata: new Set()
          };
          
          this.providers.set(providerId, provider);
          logger.info(`Provider connected => ${provider.name} (${providerId})`);

          // If provider claims to have existing data, request sync
          if (initialData.hasExistingData) {
            ws.send(JSON.stringify({
              type: 'REQUEST_SYNC',
              fullSync: true
            }));
          }

          ws.on('message', async (msg) => {
            try {
              const data = JSON.parse(msg.toString());
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
            logger.error(`WebSocket error for provider ${provider.name} (${providerId}):`, error);
            provider.isOnline = false;
          });
        } catch (error) {
          logger.error('Error handling initial provider connection:', error);
          ws.close();
        }
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
    if (merkleRoot) {
      logger.info(`Generated Merkle Root: ${merkleRoot} for fileHash: ${fileHash}`);
    } else {
      logger.warn(`Failed to generate Merkle Root for fileHash: ${fileHash}`);
    }
    
    
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
    if (merkleRoot) {
      logger.info(`Generated Merkle Root: ${merkleRoot} for fileHash: ${fileHash}`);
    } else {
      logger.warn(`Failed to generate Merkle Root for fileHash: ${fileHash}`);
    }
    

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
        logger.info(`Distributing chunk ${chunkHash} to providers: ${providerIds.join(', ')}`);
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

  private async getFile(fileHash: string):Promise<any | null> {
    const metadata = await this.hashStore.retrieveFileMeta(fileHash);
    if (!metadata) {
      // Try to find metadata from connected providers
      const foundMetadata = await this.searchMetadataFromProviders(fileHash);
      if (!foundMetadata) return null;
      await this.hashStore.storeFileMeta(fileHash, foundMetadata);
      return this.getFile(fileHash);
    }

    const buffer = await this.assembleFile(fileHash, metadata);
    return { buffer, metadata };
  }

  private async searchMetadataFromProviders(fileHash: string): Promise<FileMeta | null> {
    const onlineProviders = Array.from(this.providers.values())
      .filter(p => p.isOnline);
    
    const metadataPromises = onlineProviders.map(provider => 
      new Promise<FileMeta | null>((resolve) => {
        const timeout = setTimeout(() => resolve(null), 5000);
        
        const handler = async (message: any) => {
          try {
            const data = JSON.parse(message.toString());
            if (data.type === 'METADATA_RESPONSE' && data.fileHash === fileHash) {
              clearTimeout(timeout);
              provider.ws.removeListener('message', handler);
              if (data.metadata) {
                provider.knownMetadata.add(fileHash);
                resolve(data.metadata);
              } else {
                resolve(null);
              }
            }
          } catch (error) {
            resolve(null);
          }
        };

        provider.ws.on('message', handler);
        provider.ws.send(JSON.stringify({
          type: 'REQUEST_METADATA',
          fileHash
        }));
      })
    );

    const results = await Promise.all(metadataPromises);
    const validMetadata = results.find(meta => meta !== null);
    return validMetadata || null;
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
          logger.info(`Requesting chunk ${chunkHash} for fileHash: ${fileHash} from provider: ${availableProvider?.id}`);
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
  private async handleProviderSync(providerId: string, message: any) {
    const provider = this.providers.get(providerId);
    if (!provider) return;

    provider.storedChunks = new Set(message.storedHashes);
    
    // Handle metadata sync
    if (message.metadata) {
      for (const [fileHash, meta] of Object.entries(message.metadata)) {
        provider.knownMetadata.add(fileHash);
        const existingMeta = await this.hashStore.retrieveFileMeta(fileHash);
        if (!existingMeta) {
          await this.hashStore.storeFileMeta(fileHash, meta as FileMeta);
          logger.info(`Synced metadata for file ${fileHash} from provider ${provider.name}`);
        }
      }
    }

    if (message.merkleRoot !== provider.merkleRoot) {
      provider.merkleRoot = message.merkleRoot;
      logger.info(`Provider ${provider.name} updated Merkle Root: ${message.merkleRoot}`);
    }
  }

  private async handleProviderMessage(providerId: string, message: any) {
    const provider = this.providers.get(providerId);
    if (!provider) return;
  
    switch (message.type) {
      case 'SYNC_STATE_PROVIDER':
        await this.handleProviderSync(providerId, message);
        break;
      case 'CHUNK_DATA':
        await this.handleChunkData(message);
        break;
  
      case 'MERKLE_ROOT':
        if (message.root !== provider.merkleRoot) {
          provider.merkleRoot = message.root;
          logger.info(`Provider ${providerId} updated Merkle Root: ${message.root}`);
        }
        break;
  
      case 'SYNC_STATE':
        provider.storedChunks = new Set(message.storedChunks);
        break;
    }
  }
  

  private async handleChunkData(message: any) {
    const { fileHash, chunkHash, data } = message;
    logger.info(`Received chunk ${chunkHash} for fileHash: ${fileHash} from provider.`);
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