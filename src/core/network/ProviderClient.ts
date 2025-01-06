import WebSocket from 'ws';
import logger from '../utils/logger';
import { generateHash } from '../utils/cryptoutils';
import { buildMerkleTree, generateMerkleProof } from '../utils/merkleutils';
import { DataStore } from '../storage/DataStore';
import { HashStore, FileMeta } from '../storage/HashStore';
import { ProofStore } from '../storage/ProofStore';

export class ProviderClient {
  private bootHost: string;
  private bootPort: number;
  private providerName: string;
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

  constructor(bootHost: string, bootPort: number, storagePath: string, providerName: string) {
    this.bootHost = bootHost;
    this.bootPort = bootPort;
    this.providerName = providerName;
    
    this.dataStore = new DataStore(`${storagePath}/data`);
    this.hashStore = new HashStore(`${storagePath}/hashes`);
    this.proofStore = new ProofStore(`${storagePath}/proofs`);
  }

  public async start(): Promise<void> {
    await this.loadStoredData();
    await this.connectToHost();
    this.startPeriodicSync();
    logger.info(`ProviderClient started => name=${this.providerName}`);
  }

  public async stop(): Promise<void> {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    if (this.syncInterval) {
      clearInterval(this.syncInterval);
      this.syncInterval = null;
    }
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }
  }

  private async loadStoredData(): Promise<void> {
    try {
      const chunkHashes = await this.dataStore.getAllKeys();
      if (chunkHashes.length > 0) {
        const { root } = buildMerkleTree(chunkHashes.sort());
        if (root) {
          this.merkleRoot = root;
          logger.info(`Loaded stored data: ${chunkHashes.length} chunks, Merkle Root: ${this.merkleRoot}`);
        }
      }
    } catch (error) {
      logger.error('Error loading stored data:', error);
    }
  }

  private async connectToHost(): Promise<void> {
    if (this.ws) {
      this.ws.terminate();
      this.ws = null;
    }

    this.ws = new WebSocket(`ws://${this.bootHost}:${this.bootPort}`);

    this.ws.on('open', () => {
      this.connected = true;
      this.retryCount = 0;
      logger.info('Connected to host server');
      
      if (this.ws) {
        this.ws.send(JSON.stringify({
          type: 'PROVIDER_CONNECT',
          providerName: this.providerName,
          hasExistingData: true
        }));
      }
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
      if (this.ws) {
        this.ws.pong();
      }
    });
  }

  private handleDisconnect(): void {
    if (this.retryCount >= this.maxRetries) {
      logger.error('Max reconnection attempts reached');
      return;
    }

    this.retryCount++;
    const delay = this.retryDelay * Math.pow(2, this.retryCount - 1);
    
    this.reconnectTimeout = setTimeout(() => {
      logger.info(`Attempting reconnection (${this.retryCount}/${this.maxRetries})`);
      this.connectToHost().catch(error => 
        logger.error('Error during reconnection:', error)
      );
    }, delay);
  }

  private startPeriodicSync(): void {
    this.syncInterval = setInterval(() => {
      if (this.connected) {
        this.syncState().catch(error => 
          logger.error('Error during periodic sync:', error)
        );
      }
    }, 5 * 60 * 1000);
  }

  private async syncState(): Promise<void> {
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
      type: 'SYNC_STATE_PROVIDER',
      metadata,
      storedHashes,
      merkleRoot: this.merkleRoot
    }));
  }

  private async handleHostMessage(message: any): Promise<void> {
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

      case 'REQUEST_METADATA':
        await this.handleMetadataRequest(message);
        break;
    }
  }

  private async handleStoreChunk(message: any): Promise<void> {
    const { chunkHash, data, metadata } = message;
    const buffer = Buffer.from(data, 'hex');

    const computedHash = generateHash(buffer);
    if (computedHash !== chunkHash) {
      logger.error(`Hash verification failed for chunk ${chunkHash}`);
      return;
    }

    await Promise.all([
      this.dataStore.storeData(chunkHash, buffer),
      metadata ? this.hashStore.storeFileMeta(metadata.fileHash, metadata) : Promise.resolve()
    ]);

    const chunkHashes = await this.dataStore.getAllKeys();
    const { root } = buildMerkleTree(chunkHashes.sort());
    if (root) {
      this.merkleRoot = root;
      logger.info(`Stored chunk ${chunkHash} and updated Merkle root: ${this.merkleRoot}`);
    }
  }

  private async handleChunkRequest(message: any): Promise<void> {
    const { fileHash, chunkHash } = message;
    const chunk = await this.dataStore.retrieveData(chunkHash);

    if (chunk && this.ws) {
      logger.info(`Serving chunk: ${chunkHash} for file: ${fileHash}`);
      this.ws.send(JSON.stringify({
        type: 'CHUNK_DATA',
        fileHash,
        chunkHash,
        data: chunk.toString('hex')
      }));
    }
  }

  private async handleMetadataRequest(message: any): Promise<void> {
    const { fileHash } = message;
    const metadata = await this.hashStore.retrieveFileMeta(fileHash);

    if (this.ws) {
      this.ws.send(JSON.stringify({
        type: 'METADATA_RESPONSE',
        fileHash,
        metadata
      }));
    }
  }

  private async handleChunkProof(message: any): Promise<void> {
    const { chunkHash } = message;
    const chunk = await this.dataStore.retrieveData(chunkHash);

    if (!chunk || !this.ws) {
      logger.warn(`Chunk not found or no connection: ${chunkHash}`);
      return;
    }

    const allHashes = await this.dataStore.getAllKeys();
    const sortedHashes = allHashes.sort();
    const { tree, root } = buildMerkleTree(sortedHashes);
    if (!tree || !root) return;
    
    const proof = generateMerkleProof(tree, chunkHash);
    if (!proof) return;

    await this.proofStore.storeProof(chunkHash, proof);
    logger.info(`Generated Merkle proof for chunk: ${chunkHash}`);

    this.ws.send(JSON.stringify({
      type: 'CHUNK_PROOF',
      chunkHash,
      proof,
      merkleRoot: root,
      data: chunk.toString('hex')
    }));
  }

  private async handleRedistributeChunk(message: any): Promise<void> {
    const { chunkHash, targetProviders } = message;
    const chunk = await this.dataStore.retrieveData(chunkHash);
    const metadata = await this.getMetadataForChunk(chunkHash);

    if (chunk && metadata && this.ws) {
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

  public async getStats(): Promise<{
    providerName: string;
    totalChunks: number;
    totalFiles: number;
    merkleRoot: string;
    connected: boolean;
    storageSize: number;
  }> {
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
      providerName: this.providerName,
      totalChunks: chunkHashes.length,
      totalFiles: metadataKeys.length,
      merkleRoot: this.merkleRoot,
      connected: this.connected,
      storageSize: totalSize
    };
  }
}