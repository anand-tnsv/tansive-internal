package catcommon

import "crypto/rand"

type IdType int

const (
	ID_TYPE_GENERIC = iota
	ID_TYPE_NODE
	ID_TYPE_TENANT
	ID_TYPE_BILL
	ID_TYPE_USER
	ID_TYPE_PROJECT
	ID_TYPE_USERGROUP
)

const AIRLINE_CODE_LEN = 6

/**
 * GetUniqueId
 * This may not be unique, since this is randomly generated.
 * Has a practical collision probability of 1.5% in 10 million keys.
 * Retrying a couple of times in our use-case is better than having a key generation service
 * *Check uniqueness in DB before using the key
 */
type CheckFn func(string) bool

func GetUniqueId(t IdType) string {
	c, err := airlineCode(AIRLINE_CODE_LEN)
	if err != nil {
		//print error
		return ""
	}
	switch t {
	case ID_TYPE_TENANT:
		c = "T" + c
	case ID_TYPE_PROJECT:
		c = "P" + c
	default:
	}
	return c
}

// Function to generate a random alphanumeric string of a given length like Airline PNR Code
func airlineCode(length int) (string, error) {
	charSet := "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	charSetLen := len(charSet)

	randomBytes := make([]byte, length)
	_, err := rand.Read(randomBytes)
	if err != nil {
		return "", err
	}

	for i := 0; i < length; i++ {
		index := int(randomBytes[i]) % charSetLen
		randomBytes[i] = charSet[index]
	}

	// Ensure the first character is a letter (not a number)
	if randomBytes[0] >= '0' && randomBytes[0] <= '9' {
		index := int(randomBytes[0]) % 26
		randomBytes[0] = charSet[index]
	}

	return string(randomBytes), nil
}
