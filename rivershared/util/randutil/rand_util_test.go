package randutil

import (
	cryptorand "crypto/rand"
	"math/big"
	mathrand "math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewCryptoSeededConcurrentSafeRand(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	rand := NewCryptoSeededConcurrentSafeRand()

	// Hit the source with a bunch of goroutines to help suss out any problems
	// with concurrent safety (when combined with `-race`).
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 100; j++ {
				_ = rand.Intn(1984)
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestIntBetween(t *testing.T) {
	t.Parallel()

	const min, max = 5, 8
	rand := NewCryptoSeededConcurrentSafeRand()

	// Not exactly a super exhaustive test, but choose a relatively small range,
	// generate numbers and check they're within bounds, and run enough times
	// that we'd expect an offender to be generated if one was likely to be.
	for i := 0; i < int(max-min)*2; i++ {
		n := IntBetween(rand, min, max)
		require.GreaterOrEqual(t, n, min)
		require.Less(t, n, max)
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

func BenchmarkConcurrentSafeSource(b *testing.B) {
	rand := mathrand.New(newCryptoSeededConcurrentSafeSource())
	for n := 0; n < b.N; n++ {
		_ = rand.Intn(1984)
	}
}

func BenchmarkCryptoSource(b *testing.B) {
	intN := func(max int64) int64 {
		nBig, err := cryptorand.Int(cryptorand.Reader, big.NewInt(max))
		if err != nil {
			panic(err)
		}
		return nBig.Int64()
	}

	for n := 0; n < b.N; n++ {
		_ = intN(1984)
	}
}
