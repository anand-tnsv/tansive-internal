package hashlog

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"os"
	"sync"

	jsonitor "github.com/json-iterator/go"
)

var json = jsonitor.ConfigCompatibleWithStandardLibrary

type HashedLogEntry struct {
	Payload  map[string]any `json:"payload"`
	PrevHash string         `json:"prevHash"`
	Hash     string         `json:"hash"`
	HMAC     string         `json:"hmac"`
}

type HashLogWriter struct {
	file          *os.File
	path          string
	flushInterval int
	mu            sync.Mutex
	buffer        []HashedLogEntry
	prevHash      string
	hmacKey       []byte
}

func NewHashLogWriter(path string, flushInterval int) (*HashLogWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &HashLogWriter{
		file:          f,
		path:          path,
		flushInterval: flushInterval,
		buffer:        make([]HashedLogEntry, 0, flushInterval),
		hmacKey:       getHMACKey(),
	}, nil
}

func (lw *HashLogWriter) AddEntry(payload map[string]any) error {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	// Copy payload
	cloned := make(map[string]any, len(payload))
	for k, v := range payload {
		cloned[k] = v
	}

	entry := HashedLogEntry{
		Payload:  cloned,
		PrevHash: lw.prevHash,
	}

	// Compute hash of (PrevHash + Payload)
	dataToHash, err := json.Marshal(struct {
		Payload  map[string]any `json:"payload"`
		PrevHash string         `json:"prevHash"`
	}{
		Payload:  entry.Payload,
		PrevHash: entry.PrevHash,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal entry: %w", err)
	}
	hash := sha256.Sum256(dataToHash)
	entry.Hash = fmt.Sprintf("%x", hash[:])
	lw.prevHash = entry.Hash

	// Compute HMAC over (Payload + PrevHash + Hash)
	hmacInput, err := json.Marshal(struct {
		Payload  map[string]any `json:"payload"`
		PrevHash string         `json:"prevHash"`
		Hash     string         `json:"hash"`
	}{
		Payload:  entry.Payload,
		PrevHash: entry.PrevHash,
		Hash:     entry.Hash,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal HMAC input: %w", err)
	}
	h := hmac.New(sha256.New, lw.hmacKey)
	h.Write(hmacInput)
	entry.HMAC = fmt.Sprintf("%x", h.Sum(nil))

	lw.buffer = append(lw.buffer, entry)
	if len(lw.buffer) >= lw.flushInterval {
		return lw.flushLocked()
	}
	return nil
}

func (lw *HashLogWriter) flushLocked() error {
	for _, entry := range lw.buffer {
		b, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("failed to marshal entry: %w", err)
		}
		if _, err := lw.file.Write(append(b, '\n')); err != nil {
			return fmt.Errorf("failed to write entry: %w", err)
		}
	}
	lw.buffer = lw.buffer[:0]
	return nil
}

func (lw *HashLogWriter) Flush() error {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	return lw.flushLocked()
}

func (lw *HashLogWriter) Close() error {
	if err := lw.Flush(); err != nil {
		return err
	}
	return lw.file.Close()
}

// Replace this with proper key management
func getHMACKey() []byte {
	return []byte("tansive-dev-hmac-key") // ❗ Replace with secure key storage
}
