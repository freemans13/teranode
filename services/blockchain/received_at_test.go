package blockchain

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestReceivedAtStore_StampAndLookup(t *testing.T) {
	s := newReceivedAtStore(time.Minute)
	h := chainhash.HashH([]byte("h1"))

	before := time.Now()
	s.stamp(&h)
	stamp, found := s.lookup(&h)

	require.True(t, found)
	require.False(t, stamp.Before(before))
	require.True(t, time.Since(stamp) < time.Second)
}

func TestReceivedAtStore_WriteOnceSemantics(t *testing.T) {
	s := newReceivedAtStore(time.Minute)
	h := chainhash.HashH([]byte("h2"))

	s.stamp(&h)
	first, _ := s.lookup(&h)

	time.Sleep(10 * time.Millisecond)
	s.stamp(&h) // second stamp must not overwrite
	second, _ := s.lookup(&h)

	require.Equal(t, first, second, "first stamp must win; repeated inserts are no-ops")
}

func TestReceivedAtStore_LookupAbsent(t *testing.T) {
	s := newReceivedAtStore(time.Minute)
	h := chainhash.HashH([]byte("absent"))

	_, found := s.lookup(&h)
	require.False(t, found)
}

func TestReceivedAtStore_ConcurrentWrites(t *testing.T) {
	s := newReceivedAtStore(time.Minute)
	h := chainhash.HashH([]byte("concurrent"))

	done := make(chan struct{})
	for i := 0; i < 50; i++ {
		go func() {
			s.stamp(&h)
			done <- struct{}{}
		}()
	}
	for i := 0; i < 50; i++ {
		<-done
	}

	_, found := s.lookup(&h)
	require.True(t, found)
}

func TestReceivedAtStore_Expiration(t *testing.T) {
	s := newReceivedAtStore(50 * time.Millisecond)
	h := chainhash.HashH([]byte("expire"))

	s.stamp(&h)
	_, found := s.lookup(&h)
	require.True(t, found)

	time.Sleep(200 * time.Millisecond)
	_, found = s.lookup(&h)
	require.False(t, found, "entry older than TTL must have been evicted")
}
