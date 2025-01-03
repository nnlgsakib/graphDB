import level from 'level';
import logger  from '../utils/logger';

export interface FileMeta {
  fileHash: string;
  merkleRoot: string;
  chunkHashes: string[];
  chunkProviders: Record<string, string[]>;
   // Add these:
   fileName?: string;
   contentType?: string;
   fileSize?: number;
}

export class HashStore {
  private db: level.LevelDB;

  constructor(dbPath: string) {
    this.db = level(dbPath, { valueEncoding: 'json' });
    logger.info(`HashStore init: ${dbPath}`);
  }

  public async storeFileMeta(fileHash: string, meta: FileMeta): Promise<void> {
    await this.db.put(fileHash, meta);
  }

  public async retrieveFileMeta(fileHash: string): Promise<FileMeta | null> {
    try {
      const data = await this.db.get(fileHash);
      return data as FileMeta;
    } catch (err: any) {
      if (err.notFound) return null;
      throw err;
    }
  }
}
