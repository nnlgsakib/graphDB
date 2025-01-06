import * as crypto from 'crypto';
import * as dotenv from 'dotenv';

// Load environment variables
dotenv.config();

const KEY = process.env.PRIVATE_KEY as string;
const SALT = process.env.SALT as string;

if (!KEY || !SALT) {
    throw new Error('PRIVATE_KEY and SALT must be defined in the environment variables.');
}

/**
 * Encrypts data using AES-256-CTR algorithm.
 * @param data - The data to encrypt as a Buffer.
 * @returns The encrypted data as a hex string.
 */
function encrypt(data: Buffer): string {
    const keyBuffer = crypto.scryptSync(KEY, SALT, 32); // Derive a 256-bit key
    const iv = crypto.randomBytes(16); // Generate a random initialization vector (IV)
    const cipher = crypto.createCipheriv('aes-256-ctr', keyBuffer, iv);

    const encryptedData = Buffer.concat([
        cipher.update(data),
        cipher.final(),
    ]);

    // Prepend IV to the encrypted data
    const result = Buffer.concat([iv, encryptedData]);
    return result.toString('hex'); // Return as a hex string
}

/**
 * Decrypts data previously encrypted with `encrypt` function.
 * @param encrypted - The encrypted data as a hex string.
 * @returns The decrypted data as a UTF-8 string.
 */
function decrypt(encrypted: string): string {
    const encryptedBuffer = Buffer.from(encrypted, 'hex');

    // Extract the first 16 bytes as IV
    const iv = encryptedBuffer.slice(0, 16);
    const encryptedData = encryptedBuffer.slice(16);

    const keyBuffer = crypto.scryptSync(KEY, SALT, 32); // Derive the same 256-bit key
    const decipher = crypto.createDecipheriv('aes-256-ctr', keyBuffer, iv);

    const decryptedData = Buffer.concat([
        decipher.update(encryptedData),
        decipher.final(),
    ]);

    return decryptedData.toString('utf8');
}

export { encrypt, decrypt };
