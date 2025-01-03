// provider-client.ts

import WebSocket from 'ws';
import crypto from 'crypto';
import logger from '../utils/logger';
import { generateHash } from '../utils/cryptoutils';
import { buildMerkleTree, generateMerkleProof } from '../utils/merkleutils';

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

export class ProviderClient {
  private bootHost: string;
  private bootPort: number;
  private storageDir: string;
  private ws: WebSocket | null = null;
  private reconnectTimeout: NodeJS.Timeout | null = null;
  private syncInterval: NodeJS.Timeout | null = null;
  private metadata: Map<string, FileMeta> = new Map();
  private chunks: Map<string, Buffer> = new Map();
  private merkleRoot: string = '';
  private connected: boolean = false;
  private retryCount: number = 0;
  private maxRetries: number = 10;
  private retryDelay: number = 5000;

  constructor(bootHost: string, bootPort: number, storageDir: string) {
    this.bootHost = bootHost;
    this.bootPort = bootPort;
    this.storageDir = storageDir;
    this.initStorage();
  }

  private initStorage() {
    const fs = require('fs');
    if (!fs.existsSync(this.storageDir)) {
      fs.mkdirSync(this.storageDir, { recursive: true });
    }
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
      const fs = require('fs').promises;
      const metadataPath = `${this.storageDir}/metadata.json`;
      const chunksPath = `${this.storageDir}/chunks`;

      if (await fs.access(metadataPath).then(() => true).catch(() => false)) {
        const data = await fs.readFile(metadataPath, 'utf8');
        const parsed = JSON.parse(data);
        this.metadata = new Map(Object.entries(parsed));
      }

      const files = await fs.readdir(chunksPath).catch(() => []);
      for (const file of files) {
        const chunkHash = file.replace('.chunk', '');
        const data = await fs.readFile(`${chunksPath}/${file}`);
        this.chunks.set(chunkHash, data);
      }

      await this.updateMerkleRoot();
    } catch (error) {
      logger.error('Error loading stored data:', error);
    }
  }

  private async saveMetadata() {
    try {
      const fs = require('fs').promises;
      const metadataPath = `${this.storageDir}/metadata.json`;
      await fs.writeFile(
        metadataPath,
        JSON.stringify(Object.fromEntries(this.metadata))
      );
    } catch (error) {
      logger.error('Error saving metadata:', error);
    }
  }

  private async saveChunk(chunkHash: string, data: Buffer) {
    try {
      const fs = require('fs').promises;
      const chunksPath = `${this.storageDir}/chunks`;
      await fs.mkdir(chunksPath, { recursive: true });
      await fs.writeFile(`${chunksPath}/${chunkHash}.chunk`, data);
    } catch (error) {
      logger.error('Error saving chunk:', error);
    }
  }

  private async updateMerkleRoot() {
    const chunkHashes = Array.from(this.chunks.keys()).sort();
    if (chunkHashes.length === 0) {
      this.merkleRoot = '';
      return;
    }
    const { root } = buildMerkleTree(chunkHashes);
    this.merkleRoot = root;
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

    this.ws.send(JSON.stringify({
      type: 'SYNC_STATE',
      metadata: Object.fromEntries(this.metadata),
      storedHashes: Array.from(this.chunks.keys()),
      merkleRoot: this.merkleRoot
    }));
  }

  private async handleHostMessage(message: any) {
    switch (message.type) {
      case 'STORE_CHUNK':
        await this.handleStoreChunk(message);
        break;

      case 'REQUEST_CHUNK_DATA':
        await this.handleChunkRequest(message);
        break;

      case 'REQUEST_CHUNK_PROOF':
        await this.handleChunkProof(message);
        break;

      case 'SYNC_METADATA':
        await this.handleMetadataSync(message);
        break;

      case 'REQUEST_SYNC':
        this.syncState();
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

    this.chunks.set(chunkHash, buffer);
    await this.saveChunk(chunkHash, buffer);

    if (metadata) {
      this.metadata.set(metadata.fileHash, metadata);
      await this.saveMetadata();
    }

    await this.updateMerkleRoot();
    this.broadcastMetadataUpdate(metadata);
  }

  private async handleChunkRequest(message: any) {
    const { fileHash, chunkHash } = message;
    const chunk = this.chunks.get(chunkHash);

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
    const chunk = this.chunks.get(chunkHash);

    if (!chunk) {
      logger.warn(`Chunk not found: ${chunkHash}`);
      return;
    }

    const allHashes = Array.from(this.chunks.keys()).sort();
    const { tree, root } = buildMerkleTree(allHashes);
    const proof = generateMerkleProof(tree, chunkHash);

    this.ws?.send(JSON.stringify({
      type: 'CHUNK_PROOF',
      chunkHash,
      proof,
      merkleRoot: root,
      data: chunk.toString('hex')
    }));
  }

  private async handleMetadataSync(message: any) {
    const { fileHash, metadata } = message;
    this.metadata.set(fileHash, metadata);
    await this.saveMetadata();
  }

  private broadcastMetadataUpdate(metadata: FileMeta) {
    if (!this.ws || !this.connected) return;

    this.ws.send(JSON.stringify({
      type: 'METADATA_UPDATE',
      fileHash: metadata.fileHash,
      metadata
    }));
  }

  public getStats() {
    return {
      totalChunks: this.chunks.size,
      totalFiles: this.metadata.size,
      merkleRoot: this.merkleRoot,
      connected: this.connected,
      storageSize: Array.from(this.chunks.values())
        .reduce((acc, chunk) => acc + chunk.length, 0)
    };
  }
}