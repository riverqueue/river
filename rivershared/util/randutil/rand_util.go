package randutil

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	mathrand "math/rand"
	"sync"
	"time"
)

// NewCryptoSeededConcurrentSafeSource generates a new pseudo-random source
// that's been created with a cryptographically secure seed to ensure reasonable
// distribution of randomness between nodes and services, and wrapped so that
// access to it is concurrent safe.
//
// This project uses this technique instead of falling back on crypto/rand
// because uses of randomness don't need to be cryptographically secure, and the
// non-crypto variant is about twenty times faster.
func NewCryptoSeededConcurrentSafeRand() *mathrand.Rand {
	return mathrand.New(newCryptoSeededConcurrentSafeSource())
}

// DurationBetween generates a random duration in the range of [lowerLimit, upperLimit).
//
// TODO: When we drop Go 1.21 support, switch to `math/rand/v2` and kill the
// `rand.Rand` argument.
func DurationBetween(rand *mathrand.Rand, lowerLimit, upperLimit time.Duration) time.Duration {
	return time.Duration(IntBetween(rand, int(lowerLimit), int(upperLimit)))
}

func Hex(length int) string {
	bytes := make([]byte, length)
	if _, err := cryptorand.Read(bytes); err != nil {
		panic(err)
	}
	return hex.EncodeToString(bytes)
}

// IntBetween generates a random number in the range of [lowerLimit, upperLimit).
//
// TODO: When we drop Go 1.21 support, switch to `math/rand/v2` and kill the
// `rand.Rand` argument.
func IntBetween(rand *mathrand.Rand, lowerLimit, upperLimit int) int {
	return rand.Intn(upperLimit-lowerLimit) + lowerLimit
}

func newCryptoSeededConcurrentSafeSource() mathrand.Source {
	var seed int64

	if err := binary.Read(cryptorand.Reader, binary.BigEndian, &seed); err != nil {
		panic(err)
	}

	return &concurrentSafeSource{innerSource: mathrand.NewSource(seed)}
}

type concurrentSafeSource struct {
	innerSource mathrand.Source
	mu          sync.Mutex
}

func (s *concurrentSafeSource) Int63() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.innerSource.Int63()
}

func (s *concurrentSafeSource) Seed(seed int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.innerSource.Seed(seed)
}
