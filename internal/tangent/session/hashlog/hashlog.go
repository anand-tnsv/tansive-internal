package hashlog

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"os"
	"sync"

	jsonitor "github.com/json-iterator/go"
)

var json = jsonitor.ConfigCompatibleWithStandardLibrary

type HashedLogEntry struct {
	Payload   map[string]any `json:"payload"`
	PrevHash  string         `json:"prevHash"`
	Hash      string         `json:"hash"`
	Signature string         `json:"signature"`
}

type HashLogWriter struct {
	file          *os.File
	path          string
	flushInterval int
	mu            sync.Mutex
	buffer        []HashedLogEntry
	prevHash      string
	privKey       []byte
	closed        bool
}

func NewHashLogWriter(path string, flushInterval int, privKey []byte) (*HashLogWriter, error) {
	if len(privKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid private key: must be %d bytes, got %d", ed25519.PrivateKeySize, len(privKey))
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &HashLogWriter{
		file:          f,
		path:          path,
		flushInterval: flushInterval,
		buffer:        make([]HashedLogEntry, 0, flushInterval),
		privKey:       privKey,
	}, nil
}

func (lw *HashLogWriter) AddEntry(payload map[string]any) error {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	cloned := make(map[string]any, len(payload))
	for k, v := range payload {
		cloned[k] = v
	}

	entry := HashedLogEntry{
		Payload:  cloned,
		PrevHash: lw.prevHash,
	}

	// Compute hash
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

	// Sign (Payload + PrevHash + Hash)
	signInput, err := json.Marshal(struct {
		Payload  map[string]any `json:"payload"`
		PrevHash string         `json:"prevHash"`
		Hash     string         `json:"hash"`
	}{
		Payload:  entry.Payload,
		PrevHash: entry.PrevHash,
		Hash:     entry.Hash,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal sign input: %w", err)
	}
	signature := ed25519.Sign(lw.privKey, signInput)
	entry.Signature = base64.StdEncoding.EncodeToString(signature)

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
	lw.mu.Lock()
	defer lw.mu.Unlock()

	if lw.closed {
		return nil
	}

	if err := lw.flushLocked(); err != nil {
		return err
	}

	err := lw.file.Close()
	lw.closed = true
	return err
}
