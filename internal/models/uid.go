package models

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

func GenerateUniqueID(originPrefix string) string {
	// P = 1.47E-47 for identical ID, which is impossible on current hardware :D
	randomBytes := make([]byte, 16)
	_, err := rand.Read(randomBytes)
	if err != nil {
		return ""
	}
	return originPrefix + "-" + fmt.Sprintf("%d", time.Now().UnixNano()) + "-" + hex.EncodeToString(randomBytes)
}
