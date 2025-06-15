package hashlog

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"io"
)

func VerifyHashedLog(r io.Reader, hmacKey []byte) error {
	scanner := bufio.NewScanner(r)
	lineNum := 0
	expectedPrevHash := ""

	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()

		var entry HashedLogEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return fmt.Errorf("line %d: invalid JSON: %w", lineNum, err)
		}

		// Verify hash
		hashInput := struct {
			Payload  map[string]any `json:"payload"`
			PrevHash string         `json:"prevHash"`
		}{
			Payload:  entry.Payload,
			PrevHash: entry.PrevHash,
		}
		hashData, err := json.Marshal(hashInput)
		if err != nil {
			return fmt.Errorf("line %d: failed to marshal hash input: %w", lineNum, err)
		}
		computedHash := fmt.Sprintf("%x", sha256.Sum256(hashData))
		if entry.Hash != computedHash {
			return fmt.Errorf("line %d: hash mismatch", lineNum)
		}

		// Verify hash chain
		if entry.PrevHash != expectedPrevHash {
			return fmt.Errorf("line %d: prevHash mismatch", lineNum)
		}

		// Verify HMAC
		hmacInput := struct {
			Payload  map[string]any `json:"payload"`
			PrevHash string         `json:"prevHash"`
			Hash     string         `json:"hash"`
		}{
			Payload:  entry.Payload,
			PrevHash: entry.PrevHash,
			Hash:     entry.Hash,
		}
		hmacData, err := json.Marshal(hmacInput)
		if err != nil {
			return fmt.Errorf("line %d: failed to marshal HMAC input: %w", lineNum, err)
		}
		mac := hmac.New(sha256.New, hmacKey)
		mac.Write(hmacData)
		expectedHMAC := fmt.Sprintf("%x", mac.Sum(nil))
		if entry.HMAC != expectedHMAC {
			return fmt.Errorf("line %d: HMAC mismatch", lineNum)
		}

		expectedPrevHash = entry.Hash
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read stream: %w", err)
	}

	return nil
}
