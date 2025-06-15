package hashlog

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHashLogWriter(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.tlog")

	writer, err := NewHashLogWriter(logPath, 3)
	require.NoError(t, err)
	defer writer.Close()

	// Add some entries
	entries := []map[string]any{
		{"event": "start", "id": 1},
		{"event": "progress", "id": 2},
		{"event": "end", "id": 3},
		{"event": "summary", "id": 4},
	}

	for _, e := range entries {
		require.NoError(t, writer.AddEntry(e))
	}

	// Force flush remaining entries
	require.NoError(t, writer.Flush())

	// Read file back
	file, err := os.Open(logPath)
	require.NoError(t, err)
	defer file.Close()

	scanner := bufio.NewScanner(file)

	var prevHash string
	hmacKey := []byte("tansive-dev-hmac-key")

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		var entry HashedLogEntry
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &entry), "line %d unmarshal", lineNum)

		// Recompute hash
		expectedHashInput, _ := json.Marshal(struct {
			Payload  map[string]any `json:"payload"`
			PrevHash string         `json:"prevHash"`
		}{
			Payload:  entry.Payload,
			PrevHash: entry.PrevHash,
		})
		hash := sha256.Sum256(expectedHashInput)
		expectedHash := fmt.Sprintf("%x", hash[:])
		require.Equal(t, expectedHash, entry.Hash, "line %d hash mismatch", lineNum)

		// Recompute HMAC
		hmacInput, _ := json.Marshal(struct {
			Payload  map[string]any `json:"payload"`
			PrevHash string         `json:"prevHash"`
			Hash     string         `json:"hash"`
		}{
			Payload:  entry.Payload,
			PrevHash: entry.PrevHash,
			Hash:     entry.Hash,
		})
		h := hmac.New(sha256.New, hmacKey)
		h.Write(hmacInput)
		expectedHMAC := fmt.Sprintf("%x", h.Sum(nil))
		require.Equal(t, expectedHMAC, entry.HMAC, "line %d HMAC mismatch", lineNum)

		// Validate chaining
		require.Equal(t, prevHash, entry.PrevHash, "line %d chaining mismatch", lineNum)
		prevHash = entry.Hash
	}
	require.NoError(t, scanner.Err())
}
