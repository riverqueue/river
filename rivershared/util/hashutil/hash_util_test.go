package hashutil

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdvisoryLockHash(t *testing.T) {
	t.Parallel()

	t.Run("32BitWithPrefix", func(t *testing.T) {
		t.Parallel()

		hash := NewAdvisoryLockHash(math.MaxInt32)
		hash.Write([]byte("some content for hashing purposes"))
		key := hash.Key()
		require.Equal(t, math.MaxInt32, int(key>>32))
		require.Equal(t, 764501624, int(key&0xffffffff))
	})

	t.Run("64Bit", func(t *testing.T) {
		t.Parallel()

		hash := NewAdvisoryLockHash(0)
		hash.Write([]byte("some content for hashing purposes"))
		key := hash.Key()
		require.Equal(t, int64(1854036014321452184), key)

		// The output hash isn't guaranteed to be larger than MaxInt32 of
		// course, but given a reasonable hash function like FNV that produces
		// good distribution, it's far more likely to be than not. We're using a
		// fixed input in this test, so we know that it'll always be in this case.
		require.Greater(t, int(key), math.MaxInt32)
	})
}
