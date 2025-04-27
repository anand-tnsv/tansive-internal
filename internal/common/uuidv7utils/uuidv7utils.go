package uuidv7utils

import (
	"encoding/binary"
	"time"

	"github.com/google/uuid"
)

// UUID7 generates a new UUIDv7 and returns it.
func UUID7() uuid.UUID {
	uuidv7, _ := uuid.NewV7()
	return uuidv7
}

// GetTimestampFromUUID extracts the timestamp from a UUIDv7 and returns it as a time.Time.
func GetTimestampFromUUID(u uuid.UUID) time.Time {
	tsMillis := binary.BigEndian.Uint64(u[0:8]) >> 16 // Top 48 bits = timestamp in milliseconds
	return time.UnixMilli(int64(tsMillis))
}

// CompareUUIDv7 compares two UUIDv7 values.
// Returns:
//
//	-1 if a was created before b
//	 0 if a == b
//	+1 if a was created after b
func CompareUUIDv7(a, b uuid.UUID) int {
	for i := 0; i < len(a); i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

// IsBefore returns true if a was created before b.
func IsBefore(a, b uuid.UUID) bool {
	return CompareUUIDv7(a, b) == -1
}

// IsAfter returns true if a was created after b.
func IsAfter(a, b uuid.UUID) bool {
	return CompareUUIDv7(a, b) == 1
}
