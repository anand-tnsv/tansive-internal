package uuid

import (
	"encoding/binary"
	"time"

	"github.com/google/uuid"
)

// UUID represents a UUID
type UUID = uuid.UUID

// UUID7 generates a new UUIDv7 and returns it
func UUID7() UUID {
	uuidv7, _ := uuid.NewV7()
	return uuidv7
}

// NewRandom returns a new random (version 7) UUID
func NewRandom() (UUID, error) {
	return uuid.NewV7()
}

// New returns a new random (version 7) UUID
func New() UUID {
	uuidv7, err := uuid.NewV7()
	if err != nil {
		panic(err)
	}
	return uuidv7
}

// Parse parses a UUID string
func Parse(s string) (UUID, error) {
	return uuid.Parse(s)
}

// MustParse parses a UUID string and panics if the string is not a valid UUID
func MustParse(s string) UUID {
	return uuid.MustParse(s)
}

// IsUUIDv7 checks if the given UUID is a valid UUIDv7
func IsUUIDv7(id UUID) bool {
	return id.Version() == uuid.Version(7)
}

// GetTimestampFromUUID extracts the timestamp from a UUIDv7 and returns it as a time.Time
func GetTimestampFromUUID(u UUID) time.Time {
	tsMillis := binary.BigEndian.Uint64(u[0:8]) >> 16 // Top 48 bits = timestamp in milliseconds
	return time.UnixMilli(int64(tsMillis))
}

// CompareUUIDv7 compares two UUIDv7 values.
// Returns:
//
//	-1 if a was created before b
//	 0 if a == b
//	+1 if a was created after b
func CompareUUIDv7(a, b UUID) int {
	for i := range a {
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
func IsBefore(a, b UUID) bool {
	return CompareUUIDv7(a, b) == -1
}

// IsAfter returns true if a was created after b.
func IsAfter(a, b UUID) bool {
	return CompareUUIDv7(a, b) == 1
}

// Nil is the zero UUID
var Nil = uuid.Nil
