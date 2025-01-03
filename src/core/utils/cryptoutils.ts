import crypto from 'crypto';

/**
 * Prefix with 'nlg' after SHA256 hashing.
 */


export function generateRandomHash(): string {
  // Create 32 bytes of randomness => 256 bits
  const randomHex = crypto.randomBytes(32).toString('hex');
  return `nlg${randomHex}`.slice(0, 43);
}
export function generateHash(data: string | Buffer): string {
    const hash = crypto.createHash('sha256').update(data).digest('hex');
   return `nlg${hash}`.slice(0, 20);
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