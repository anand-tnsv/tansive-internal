package catcommon

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"

	"golang.org/x/crypto/argon2"
)

const (
	saltSize    = 16
	keySize     = 32
	nonceSize   = 12
	memory      = 64 * 1024
	iterations  = 1
	parallelism = 4
)

// Derives a 32-byte key from a password and salt using Argon2id
func deriveKey(password, salt []byte) []byte {
	return argon2.IDKey(password, salt, iterations, memory, uint8(parallelism), keySize)
}

// Encrypts raw binary data with a password using Argon2id + AES-GCM
func Encrypt(data []byte, password string) ([]byte, error) {
	salt := make([]byte, saltSize)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}

	key := deriveKey([]byte(password), salt)

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, nonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	ciphertext := aesgcm.Seal(nil, nonce, data, nil)

	// Format: [salt|nonce|ciphertext]
	result := append(salt, nonce...)
	result = append(result, ciphertext...)
	return result, nil
}

// Decrypts the encrypted blob using the password
func Decrypt(blob []byte, password string) ([]byte, error) {
	if len(blob) < saltSize+nonceSize {
		return nil, fmt.Errorf("invalid blob length")
	}

	salt := blob[:saltSize]
	nonce := blob[saltSize : saltSize+nonceSize]
	ciphertext := blob[saltSize+nonceSize:]

	key := deriveKey([]byte(password), salt)

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	plaintext, err := aesgcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}
