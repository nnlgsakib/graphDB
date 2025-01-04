import WebSocket from 'ws';
import logger from '../utils/logger';
import { generateHash } from '../utils/cryptoutils';
import { buildMerkleTree, generateMerkleProof } from '../utils/merkleutils';
import { DataStore } from './../storage/DataStore';
import { HashStore, FileMeta } from './../storage/HashStore';
import { ProofStore } from '../storage/ProofStore';

export class ProviderClient {
  private bootHost: string;
  private bootPort: number;
  private ws: WebSocket | null = null;
  private reconnectTimeout: NodeJS.Timeout | null = null;
  private syncInterval: NodeJS.Timeout | null = null;
  private connected: boolean = false;
  private retryCount: number = 0;
  private maxRetries: number = 10;
  private retryDelay: number = 5000;
  private merkleRoot: string = '';

  private dataStore: DataStore;
  private hashStore: HashStore;
  private proofStore: ProofStore;

  constructor(bootHost: string, bootPort: number, storagePath: string) {
    this.bootHost = bootHost;
    this.bootPort = bootPort;
    
    // Initialize LevelDB stores
    this.dataStore = new DataStore(`${storagePath}/data`);
    this.hashStore = new HashStore(`${storagePath}/hashes`);
    this.proofStore = new ProofStore(`${storagePath}/proofs`);
  }

  public async start() {
    await this.loadStoredData();
    await this.connectToHost();
    this.startPeriodicSync();
  }

  public async stop() {
    if (this.ws) {
      this.ws.close();
    }
    if (this.syncInterval) {
      clearInterval(this.syncInterval);
    }
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
    }
  }

  private async loadStoredData() {
    try {
      const chunkHashes = await this.dataStore.getAllKeys();
      if (chunkHashes.length > 0) {
        const { root } = buildMerkleTree(chunkHashes.sort());
        this.merkleRoot = root;
      }
    } catch (error) {
      logger.error('Error loading stored data:', error);
    }
  }

  private async updateMerkleRoot() {
    const chunkHashes = await this.dataStore.getAllKeys();
    if (chunkHashes.length === 0) {
      this.merkleRoot = '';
      return;
    }
    const { root } = buildMerkleTree(chunkHashes.sort());
    this.merkleRoot = root;
    
    this.ws?.send(JSON.stringify({
      type: 'MERKLE_ROOT',
      root: this.merkleRoot
    }));
  }

  private startPeriodicSync() {
    this.syncInterval = setInterval(() => {
      if (this.connected) {
        this.syncState();
      }
    }, 5 * 60 * 1000);
  }

  private async connectToHost() {
    if (this.ws) {
      this.ws.terminate();
    }

    this.ws = new WebSocket(`ws://${this.bootHost}:${this.bootPort}`);

    this.ws.on('open', () => {
      this.connected = true;
      this.retryCount = 0;
      logger.info('Connected to host');
      this.syncState();
    });

    this.ws.on('message', async (message) => {
      try {
        const msg = JSON.parse(message.toString());
        await this.handleHostMessage(msg);
      } catch (error) {
        logger.error('Error handling host message:', error);
      }
    });

    this.ws.on('close', () => {
      this.connected = false;
      this.handleDisconnect();
    });

    this.ws.on('error', (error) => {
      logger.error('WebSocket error:', error);
      this.connected = false;
      this.handleDisconnect();
    });

    this.ws.on('ping', () => {
      this.ws?.pong();
    });
  }

  private handleDisconnect() {
    if (this.retryCount >= this.maxRetries) {
      logger.error('Max reconnection attempts reached');
      return;
    }

    this.retryCount++;
    const delay = this.retryDelay * Math.pow(2, this.retryCount - 1);
    
    this.reconnectTimeout = setTimeout(() => {
      logger.info(`Attempting reconnection (${this.retryCount}/${this.maxRetries})`);
      this.connectToHost();
    }, delay);
  }

  private async syncState() {
    if (!this.ws || !this.connected) return;

    const storedHashes = await this.dataStore.getAllKeys();
    const metadataKeys = await this.hashStore.getAllFileMetaKeys();
    const metadata: Record<string, FileMeta> = {};
    
    for (const key of metadataKeys) {
      const meta = await this.hashStore.retrieveFileMeta(key);
      if (meta) {
        metadata[key] = meta;
      }
    }

    this.ws.send(JSON.stringify({
      type: 'SYNC_STATE',
      metadata,
      storedHashes,
      merkleRoot: this.merkleRoot
    }));
  }

  private async handleHostMessage(message: any) {
    switch (message.type) {
      case 'STORE_CHUNK':
        await this.handleStoreChunk(message);
        break;

      case 'REQUEST_CHUNK':
        await this.handleChunkRequest(message);
        break;

      case 'REQUEST_CHUNK_PROOF':
        await this.handleChunkProof(message);
        break;

      case 'REDISTRIBUTE_CHUNK':
        await this.handleRedistributeChunk(message);
        break;

      case 'REQUEST_SYNC':
        await this.syncState();
        break;
    }
  }

  private async handleStoreChunk(message: any) {
    const { chunkHash, data, metadata } = message;
    const buffer = Buffer.from(data, 'hex');

    // Verify chunk hash
    const computedHash = generateHash(buffer);
    if (computedHash !== chunkHash) {
      logger.error(`Hash verification failed for chunk ${chunkHash}`);
      return;
    }

    // Store chunk data and metadata
    await Promise.all([
      this.dataStore.storeData(chunkHash, buffer),
      metadata ? this.hashStore.storeFileMeta(metadata.fileHash, metadata) : Promise.resolve()
    ]);

    // Update Merkle root
    await this.updateMerkleRoot();
  }

  private async handleChunkRequest(message: any) {
    const { fileHash, chunkHash } = message;
    const chunk = await this.dataStore.retrieveData(chunkHash);

    if (chunk) {
      this.ws?.send(JSON.stringify({
        type: 'CHUNK_DATA',
        fileHash,
        chunkHash,
        data: chunk.toString('hex')
      }));
    }
  }

  private async handleChunkProof(message: any) {
    const { chunkHash } = message;
    const chunk = await this.dataStore.retrieveData(chunkHash);

    if (!chunk) {
      logger.warn(`Chunk not found: ${chunkHash}`);
      return;
    }

    const allHashes = await this.dataStore.getAllKeys();
    const sortedHashes = allHashes.sort();
    const { tree, root } = buildMerkleTree(sortedHashes);
    const proof = generateMerkleProof(tree, chunkHash);

    await this.proofStore.storeProof(chunkHash, proof);

    this.ws?.send(JSON.stringify({
      type: 'CHUNK_PROOF',
      chunkHash,
      proof,
      merkleRoot: root,
      data: chunk.toString('hex')
    }));
  }
  private async handleRedistributeChunk(message: any) {
    const { chunkHash, targetProviders } = message;
    const chunk = await this.dataStore.retrieveData(chunkHash);
    const metadata = await this.getMetadataForChunk(chunkHash);

    if (chunk && metadata) {
      targetProviders.forEach((providerId: string) => {
        this.ws?.send(JSON.stringify({
          type: 'FORWARD_CHUNK',
          targetProvider: providerId,
          chunkHash,
          data: chunk.toString('hex'),
          metadata
        }));
      });
    }
  }


  private async getMetadataForChunk(chunkHash: string): Promise<FileMeta | null> {
    const metadataKeys = await this.hashStore.getAllFileMetaKeys();
    for (const key of metadataKeys) {
      const meta = await this.hashStore.retrieveFileMeta(key);
      if (meta && meta.chunkHashes.includes(chunkHash)) {
        return meta;
      }
    }
    return null;
  }

  public async getStats() {
    const chunkHashes = await this.dataStore.getAllKeys();
    const metadataKeys = await this.hashStore.getAllFileMetaKeys();
    let totalSize = 0;

    for (const hash of chunkHashes) {
      const chunk = await this.dataStore.retrieveData(hash);
      if (chunk) {
        totalSize += chunk.length;
      }
    }

    return {
      totalChunks: chunkHashes.length,
      totalFiles: metadataKeys.length,
      merkleRoot: this.merkleRoot,
      connected: this.connected,
      storageSize: totalSize
    };
  }
}