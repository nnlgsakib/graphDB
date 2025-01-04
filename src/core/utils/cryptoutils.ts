import crypto from 'crypto';

/**
 * Generate a purely random 256-bit hash (used e.g. for fileHash).
 * We prefix with `nlg` to maintain your naming style.
 */
export function generateRandomHash(): string {
  const randomHex = crypto.randomBytes(32).toString('hex'); // 64 hex chars
  return `nlg${randomHex}`.slice(0, 35); // Trim to 64 chars
}

/**
 * Generate a deterministic hash for chunk data.
 * Removed the random suffix to ensure same data produces same hash.
 * This ensures consistency between host and provider verification.
 */
export function generateHash(data: string | Buffer): string {
  const shaPart = crypto.createHash('sha256').update(data).digest('hex');
  return `nlg${shaPart}`.slice(0, 35);
}

/**
 * Generate a unique hash for chunk data by combining the data hash with a timestamp.
 * Use this when you need unique hashes even for identical chunks.
 */
export function generateUniqueHash(data: string | Buffer): string {
  const timestamp = Date.now().toString();
  const combinedData = Buffer.concat([
    Buffer.isBuffer(data) ? data : Buffer.from(data),
    Buffer.from(timestamp)
  ]);
  const shaPart = crypto.createHash('sha256').update(combinedData).digest('hex');
  return `nlg${shaPart}`;
}

/**
 * Split a Buffer into fixed-size chunks.
 */
export function chunkBuffer(data: Buffer, chunkSize: number): Buffer[] {
  const chunks: Buffer[] = [];
  let offset = 0;
  while (offset < data.length) {
    const end = Math.min(offset + chunkSize, data.length);
    chunks.push(data.slice(offset, end));
    offset = end;
  }
  return chunks;
}