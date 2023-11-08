package hashutil

import (
	"hash"
	"hash/fnv"
)

// AdvisoryLockHash's job is to produce a pair of keys that'll be used with
// Postgres advisory lock functions like `pg_advisory_xact_lock`.
//
// Advisory locks all share a single 64-bit number space, so it's technically
// possible for completely unrelated locks to collide with each other. It's
// quite unlikely, but this type allows a user to configure a specific 32-bit
// prefix which they can use to guarantee that no River hash will ever conflict
// with one from their application. If no prefix is configured, it'll use the
// entire 64-bit number space.
type AdvisoryLockHash struct {
	configuredPrefix int32
	hash32           hash.Hash32
	hash64           hash.Hash64
}

func NewAdvisoryLockHash(configuredPrefix int32) *AdvisoryLockHash {
	return &AdvisoryLockHash{
		configuredPrefix: configuredPrefix,

		// Not technically required to initialize both, but these just return
		// constants, so overhead is ~zero, and it looks nicer.
		hash32: fnv.New32(),
		hash64: fnv.New64(),
	}
}

// Key generates a pair of keys to be passed into Postgres advisory lock
// functions like `pg_advisory_xact_lock`.
func (h *AdvisoryLockHash) Key() int64 {
	if h.configuredPrefix == 0 {
		return int64(h.hash64.Sum64()) // overflow allowed
	}

	return int64(uint64(h.configuredPrefix)<<32 | uint64(h.hash32.Sum32())) // overflow allowed
}

// Write writes bytes to the underlying hash.
func (h *AdvisoryLockHash) Write(data []byte) {
	var activeHash hash.Hash = h.hash32
	if h.configuredPrefix == 0 {
		activeHash = h.hash64
	}

	_, err := activeHash.Write(data)
	if err != nil {
		panic(err) // error basically impossible, so panic for caller convenience
	}
}
