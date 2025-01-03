import elliptic from 'elliptic';
import CryptoJS, { lib } from 'crypto-js';
import dotenv from 'dotenv';
import logger from './logger';

dotenv.config();

const EC = new elliptic.ec('secp256k1');

const PRIVATE_KEY = process.env.PRIVATE_KEY || '';
if (!PRIVATE_KEY) {
  logger.error('No PRIVATE_KEY in environment. Exiting...');
  process.exit(1);
}

export const keyPair = EC.keyFromPrivate(PRIVATE_KEY, 'hex');

/**
 * AES-GCM (actually CBC in the code) encryption with ephemeral ECC key.
 */
export function encryptData(plaintext: string): string {
  // ephemeral key
  const ephemeralKey = EC.genKeyPair();
  const ephemeralPub = ephemeralKey.getPublic(true, 'array');

  // derive secret
  const sharedSecret = ephemeralKey
    .derive(keyPair.getPublic())
    .toArrayLike(Buffer, 'be', 32);

  // 12-byte IV
  const iv = CryptoJS.lib.WordArray.random(12);

  // Encrypt with AES (currently using CBC mode)
  const encrypted = CryptoJS.AES.encrypt(
    plaintext,
    CryptoJS.enc.Hex.parse(sharedSecret.toString('hex')),
    {
      iv,
      mode: CryptoJS.mode.CBC,
    }
  );

  // Convert ephemeral public key, IV, and ciphertext to Buffers
  const ephemeralPubBuf = Buffer.from(ephemeralPub);
  const ivBuf = Buffer.from(iv.toString(CryptoJS.enc.Hex), 'hex');
  const cipherHex = encrypted.ciphertext.toString(CryptoJS.enc.Hex);
  const cipherBuf = Buffer.from(cipherHex, 'hex');

  // Combine ephemeral pub + IV + ciphertext
  const payload = Buffer.concat([ephemeralPubBuf, ivBuf, cipherBuf]);
  return payload.toString('hex');
}

/**
 * AES-GCM (actually CBC in the code) decryption with static ECC key.
 */
export function decryptData(hexPayload: string): string {
  const payload = Buffer.from(hexPayload, 'hex');

  // ephemeralPub(33 bytes) + IV(12 bytes) + cipher
  const ephemeralPubKey = payload.slice(0, 33);
  const ivBuf = payload.slice(33, 45);
  const cipherBuf = payload.slice(45);

  // Reconstruct ephemeral public key
  const ephemeralPub = EC.keyFromPublic(ephemeralPubKey, 'array');

  // Derive the same shared secret
  const sharedSecret = ephemeralPub
    .derive(keyPair.getPublic())
    .toArrayLike(Buffer, 'be', 32);

  // Convert cipher and IV to CryptoJS WordArray
  const cipherWordArray = CryptoJS.enc.Hex.parse(cipherBuf.toString('hex'));
  const ivWordArray = CryptoJS.enc.Hex.parse(ivBuf.toString('hex'));
  const keyWordArray = CryptoJS.enc.Hex.parse(sharedSecret.toString('hex'));

  // Create valid CipherParams from ciphertext
  const cipherParams = lib.CipherParams.create({
    ciphertext: cipherWordArray,
  });

  // Decrypt
  const decrypted = CryptoJS.AES.decrypt(cipherParams, keyWordArray, {
    iv: ivWordArray,
    mode: CryptoJS.mode.CBC,
  });

  return decrypted.toString(CryptoJS.enc.Utf8);
}
