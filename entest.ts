import {encrypt , decrypt} from './src/core/utils/encryption';

// Testing the encryption and decryption
function testEncryption() {
    const data = 'This is a secret message!';
    const key = 'my-secret-key';
    const salt = 'unique-salt';

    console.log('Original Data:', data);

    // Encrypt the data
    const encryptedData = encrypt(data);
    console.log('Encrypted Data:', encryptedData);

    // Decrypt the data
    const decryptedData = decrypt(encryptedData);
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
