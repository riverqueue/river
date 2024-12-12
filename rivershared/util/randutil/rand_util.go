package randutil

import (
	cryptorand "crypto/rand"
	"encoding/hex"
	"math/rand/v2"
	"time"
)

// DurationBetween generates a random duration in the range of [lowerLimit, upperLimit).
func DurationBetween(lowerLimit, upperLimit time.Duration) time.Duration {
	return time.Duration(IntBetween(int(lowerLimit), int(upperLimit)))
}

func Hex(length int) string {
	bytes := make([]byte, length)
	if _, err := cryptorand.Read(bytes); err != nil {
		panic(err)
	}
	return hex.EncodeToString(bytes)
}

// IntBetween generates a random number in the range of [lowerLimit, upperLimit).
func IntBetween(lowerLimit, upperLimit int) int {
	return rand.IntN(upperLimit-lowerLimit) + lowerLimit
}
