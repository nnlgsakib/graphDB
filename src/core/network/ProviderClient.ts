import WebSocket from 'ws';
import { DataStore } from '../storage/DataStore';
import { HashStore } from '../storage/HashStore';
import { ProofStore } from '../storage/ProofStore';
import { generateHash } from '../utils/cryptoutils';
import { buildMerkleTree, generateMerkleProof } from '../utils/merkleutils';
import logger from '../utils/logger';

export class ProviderClient {
  private bootHost: string;
  private bootPort: number;

  private dataStore: DataStore;
  private hashStore: HashStore;
  private proofStore: ProofStore;

  private ws: WebSocket | null = null;
  private merkleRoot: string = '';
  private merkleTree: any = null;

  // Build new Merkle root every 10 min
  private merkleIntervalMs = 10 * 60 * 1000;
  private merkleIntervalId: NodeJS.Timeout | null = null;

  constructor(bootHost: string, bootPort: number, dbDir: string) {
    this.bootHost = bootHost;
    this.bootPort = bootPort;
    this.dataStore = new DataStore(`${dbDir}/data`);
    this.hashStore = new HashStore(`${dbDir}/hashes`);
    this.proofStore = new ProofStore(`${dbDir}/proofs`);
  }

  public async start() {
    await this.connectToHost();
    await this.buildAndSubmitMerkleRoot(); // initial Merkle root
    this.merkleIntervalId = setInterval(() => {
      this.buildAndSubmitMerkleRoot();
    }, this.merkleIntervalMs);

    logger.info(`ProviderClient started => host=${this.bootHost}, port=${this.bootPort}`);
  }

  private connectToHost(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(`ws://${this.bootHost}:${this.bootPort}`);

      this.ws.on('open', () => {
        logger.info('Provider => connected to Host');
        this.ws?.send(JSON.stringify({ type: 'REGISTER_PROVIDER' }));
        resolve();
      });

      this.ws.on('error', (err) => {
        logger.error('Provider => error connecting to Host', err);
        reject(err);
      });

      this.ws.on('message', async (message) => {
        const msg = message.toString();
        try {
          const parsed = JSON.parse(msg);
          switch (parsed.type) {
            case 'STORE_CHUNK':
              await this.handleStoreChunk(parsed);
              break;

            case 'REQUEST_CHUNK_PROOF':
              await this.handleRequestChunkProof(parsed);
              break;

            case 'REQUEST_CHUNK_DATA':
              await this.handleRequestChunkData(parsed);
              break;

            default:
              logger.warn(`Unknown message from host: ${parsed.type}`);
          }
        } catch (e) {
          logger.error('Provider => parse error from host message:', msg, e);
        }
      });
    });
  }

  private async buildAndSubmitMerkleRoot() {
    try {
      const allHashes = await this.dataStore.getAllKeys();
      if (allHashes.length === 0) {
        this.merkleRoot = '';
        this.ws?.send(
          JSON.stringify({
            type: 'MERKLE_UPDATE',
            root: this.merkleRoot,
            timestamp: Date.now(),
          })
        );
        logger.info('No stored chunks => empty Merkle root');
        return;
      }

      const { tree, root } = buildMerkleTree(allHashes);
      this.merkleTree = tree;
      this.merkleRoot = root;

      this.ws?.send(
        JSON.stringify({
          type: 'MERKLE_UPDATE',
          root: this.merkleRoot,
          timestamp: Date.now(),
        })
      );

      logger.info(`Provider => MERKLE_UPDATE => ${this.merkleRoot}`);
    } catch (err) {
      logger.error('Error building Merkle root:', err);
    }
  }

  /**
   * Handle chunk storage from the host
   */
  private async handleStoreChunk(parsed: any) {
    const { chunkHash, data } = parsed;
    const rawBuf = Buffer.from(data, 'hex');

    // verify chunk hash
    const computed = generateHash(rawBuf);  // Use the deterministic version
    if (computed !== chunkHash) {
      logger.error(`Hash mismatch => expected=${chunkHash}, got=${computed}`);
      return;
    }

    await this.dataStore.storeData(chunkHash, rawBuf);
    logger.info(`Provider => stored chunk ${chunkHash}`);

    // optionally rebuild Merkle
    // await this.buildAndSubmitMerkleRoot();
  }

  /**
   * The host is asking for a chunk proof
   */
  private async handleRequestChunkProof(parsed: any) {
    const { chunkHash } = parsed;
    const chunkBuf = await this.dataStore.retrieveData(chunkHash);
    if (!chunkBuf) {
      logger.warn(`Provider => missing chunk ${chunkHash}, cannot prove`);
      return;
    }
    if (!this.merkleTree) {
      logger.warn('No merkle tree built yet, skipping proof');
      return;
    }

    const proof = generateMerkleProof(this.merkleTree, chunkHash);

    this.ws?.send(
      JSON.stringify({
        type: 'CHUNK_PROOF',
        chunkHash,
        proof,
        merkleRoot: this.merkleRoot,
        // We'll still call it "encryptedHex", but it's just raw data
        encryptedHex: chunkBuf.toString('hex'),
      })
    );

    logger.info(`Provider => CHUNK_PROOF for ${chunkHash}`);
  }

  /**
   * The host wants the chunk data for reassembly
   */
  private async handleRequestChunkData(parsed: any) {
    const { fileHash, chunkHash } = parsed;
    const chunkBuf = await this.dataStore.retrieveData(chunkHash);
    if (!chunkBuf) {
      logger.warn(`Provider => missing chunk ${chunkHash}, cannot return data`);
      return;
    }

    // Send chunk data back
    this.ws?.send(
      JSON.stringify({
        type: 'CHUNK_DATA',
        fileHash,
        chunkHash,
        dataHex: chunkBuf.toString('hex'),
      })
    );

    logger.info(`Provider => CHUNK_DATA for ${chunkHash}, file=${fileHash}`);
  }
}
