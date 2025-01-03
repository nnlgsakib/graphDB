import level from 'level';
import logger  from '../utils/logger';

export class DataStore {
  private db: level.LevelDB;

  constructor(dbPath: string) {
    this.db = level(dbPath, { valueEncoding: 'binary' });
    logger.info(`DataStore init: ${dbPath}`);
  }

  public async storeData(hash: string, data: Buffer): Promise<void> {
    await this.db.put(hash, data);
  }

  public async retrieveData(hash: string): Promise<Buffer | null> {
    try {
      const val = await this.db.get(hash);
      return val as Buffer;
    } catch (err: any) {
      if (err.notFound) return null;
      throw err;
    }
  }

  public async getAllKeys(): Promise<string[]> {
    return new Promise((resolve, reject) => {
      const keys: string[] = [];
      this.db.createKeyStream()
        .on('data', (key) => keys.push(key.toString()))
        .on('error', (err) => reject(err))
        .on('end', () => resolve(keys));
    });
  }
}
