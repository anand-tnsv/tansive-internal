package common

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
)

type IdType int

const (
	ID_TYPE_GENERIC = iota
	ID_TYPE_TENANT
	ID_TYPE_USER
	ID_TYPE_PROJECT
)

const (
	AIRLINE_CODE_LEN = 6
	// Define character sets for better readability and maintainability
	LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	DIGITS  = "0123456789"
	CHARS   = LETTERS + DIGITS
)

// secureRandomInt generates a cryptographically secure random number between 0 and max
func secureRandomInt(max int) (int, error) {
	var buf [8]byte
	_, err := rand.Read(buf[:])
	if err != nil {
		return 0, fmt.Errorf("failed to generate random number: %w", err)
	}

	n := binary.BigEndian.Uint64(buf[:])
	return int(n % uint64(max)), nil
}

// GetUniqueId generates a unique ID with a prefix based on the type
// This may not be unique, since this is randomly generated.
// Has a practical collision probability of 1.5% in 10 million keys.
// Retrying a couple of times in our use-case is better than having a key generation service
// *Check uniqueness in DB before using the key
func GetUniqueId(t IdType) (string, error) {
	code, err := airlineCode(AIRLINE_CODE_LEN)
	if err != nil {
		return "", fmt.Errorf("failed to generate unique ID: %w", err)
	}

	prefix := ""
	switch t {
	case ID_TYPE_TENANT:
		prefix = "T"
	case ID_TYPE_USER:
		prefix = "U"
	case ID_TYPE_PROJECT:
		prefix = "P"
	}

	return prefix + code, nil
}

// airlineCode generates a random alphanumeric string of a given length like Airline PNR Code
func airlineCode(length int) (string, error) {
	if length <= 0 {
		return "", fmt.Errorf("length must be positive, got %d", length)
	}

	// Pre-allocate the result slice for better performance
	result := make([]byte, length)

	// First character must be a letter
	letterIdx, err := secureRandomInt(len(LETTERS))
	if err != nil {
		return "", fmt.Errorf("failed to generate first character: %w", err)
	}
	result[0] = LETTERS[letterIdx]

	// Generate remaining characters
	for i := 1; i < length; i++ {
		idx, err := secureRandomInt(len(CHARS))
		if err != nil {
			return "", fmt.Errorf("failed to generate character at position %d: %w", i, err)
		}
		result[i] = CHARS[idx]
	}

	return string(result), nil
}
