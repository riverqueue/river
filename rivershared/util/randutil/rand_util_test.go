package randutil

import (
	cryptorand "crypto/rand"
	"math/big"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDurationBetween(t *testing.T) {
	t.Parallel()

	const lowerLimit, upperLimit = 5 * time.Second, 8 * time.Second

	// Not exactly a super exhaustive test, but choose a relatively small range,
	// generate numbers and check they're within bounds, and run enough times
	// that we'd expect an offender to be generated if one was likely to be.
	for range int(upperLimit/time.Second-lowerLimit/time.Second) * 2 {
		n := DurationBetween(lowerLimit, upperLimit)
		require.GreaterOrEqual(t, n, lowerLimit)
		require.Less(t, n, upperLimit)
	}
}

func TestIntBetween(t *testing.T) {
	t.Parallel()

	const lowerLimit, upperLimit = 5, 8

	// Not exactly a super exhaustive test, but choose a relatively small range,
	// generate numbers and check they're within bounds, and run enough times
	// that we'd expect an offender to be generated if one was likely to be.
	for range int(upperLimit-lowerLimit) * 2 {
		n := IntBetween(lowerLimit, upperLimit)
		require.GreaterOrEqual(t, n, lowerLimit)
		require.Less(t, n, upperLimit)
	}
}

//
// On my Macbook, the non-crypto source is about ~20 faster:
//
// $ go test ./internal/util/randutil -bench Bench
// goos: darwin
// goarch: arm64
// pkg: github.com/riverqueue/river/internal/util/randutil
// BenchmarkConcurrentSafeSource-8         80612518                14.68 ns/op
// BenchmarkCryptoSource-8                  3806643               316.7 ns/op
// PASS
// ok      github.com/riverqueue/river/internal/util/randutil 3.552s
//

func BenchmarkRandV2(b *testing.B) {
	for range b.N {
		_ = rand.IntN(1984)
	}
}

func BenchmarkCryptoSource(b *testing.B) {
	intN := func(upperLimit int64) int64 {
		nBig, err := cryptorand.Int(cryptorand.Reader, big.NewInt(upperLimit))
		if err != nil {
			panic(err)
		}
		return nBig.Int64()
	}

	for range b.N {
		_ = intN(1984)
	}
}
