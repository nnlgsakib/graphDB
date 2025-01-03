import level from 'level';
import  logger  from '../utils/logger';

export class ProofStore {
  private db: level.LevelDB;

  constructor(dbPath: string) {
    this.db = level(dbPath, { valueEncoding: 'json' });
    logger.info(`ProofStore init: ${dbPath}`);
  }

  public async storeProof(key: string, proof: any): Promise<void> {
    await this.db.put(key, proof);
  }

  public async retrieveProof(key: string): Promise<any | null> {
    try {
      return await this.db.get(key);
    } catch (err: any) {
      if (err.notFound) return null;
      throw err;
    }
  }
}