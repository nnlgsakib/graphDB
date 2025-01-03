import * as crypto from 'crypto';

// Encryption function
function encrypt(data: string, key: string, salt: string): string {
    const keyBuffer = crypto.scryptSync(key, salt, 32); // Derive a 256-bit key
    const iv = crypto.randomBytes(16); // Generate a random IV
    const cipher = crypto.createCipheriv('aes-256-gcm', keyBuffer, iv);

    const encryptedData = Buffer.concat([
        cipher.update(data, 'utf8'),
        cipher.final(),
    ]);
    const authTag = cipher.getAuthTag(); // Get the authentication tag

    return `${iv.toString('hex')}:${encryptedData.toString('hex')}:${authTag.toString('hex')}`; // Combine IV, encrypted data, and auth tag
}

// Decryption function
function decrypt(encrypted: string, key: string, salt: string): string {
    const [ivHex, encryptedDataHex, authTagHex] = encrypted.split(':');
    if (!ivHex || !encryptedDataHex || !authTagHex) {
        throw new Error('Invalid encrypted data format');
    }

    const iv = Buffer.from(ivHex, 'hex');
    const encryptedData = Buffer.from(encryptedDataHex, 'hex');
    const authTag = Buffer.from(authTagHex, 'hex');
    const keyBuffer = crypto.scryptSync(key, salt, 32); // Derive the same 256-bit key

    const decipher = crypto.createDecipheriv('aes-256-gcm', keyBuffer, iv);
    decipher.setAuthTag(authTag); // Set the authentication tag

    const decryptedData = Buffer.concat([
        decipher.update(encryptedData),
        decipher.final(),
    ]);

    return decryptedData.toString('utf8');
}

// Testing the encryption and decryption
function testEncryption() {
    const data = 'This is a secret message!';
    const key = 'my-secret-key';
    const salt = 'unique-salt';

    console.log('Original Data:', data);

    // Encrypt the data
    const encryptedData = encrypt(data, key, salt);
    console.log('Encrypted Data:', encryptedData);

    // Decrypt the data
    const decryptedData = decrypt(encryptedData, key, salt);
    console.log('Decrypted Data:', decryptedData);

    // Assert to check if encryption and decryption are consistent
    if (data === decryptedData) {
        console.log('Encryption and Decryption successful!');
    } else {
        console.error('Mismatch in original and decrypted data!');
    }
}

// Run the test
testEncryption();
