import * as crypto from 'crypto';
import * as dotenv from 'dotenv';
dotenv.config();

const KEY = process.env.PRIVATE_KEY as string;
const SALT = process.env.SALT as string;

// Encryption function
function encrypt(data: string): string {
    const keyBuffer = crypto.scryptSync(KEY, SALT, 32); // Derive a 256-bit key
    const iv = crypto.randomBytes(16); // Generate a random IV
    const cipher = crypto.createCipheriv('aes-256-ctr', keyBuffer, iv);

    const encryptedData = Buffer.concat([
        cipher.update(data, 'utf8'),
        cipher.final(),
    ]);

    // Prepend IV to the encrypted data as a single buffer
    const result = Buffer.concat([iv, encryptedData]);
    return result.toString('hex'); // Return as a single hex string
}

// Decryption function
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
