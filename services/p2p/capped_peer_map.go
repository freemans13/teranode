package p2p

import (
	"sync"
	"sync/atomic"
)

// cappedPeerMap is a size-bounded map from announced hash to the peer that
// announced it (ban attribution). Gossip inserts are driven entirely by
// untrusted input — an announcement stores the peer BEFORE any check that the
// hash names a real block or subtree — so the bound is enforced inline at
// insert (issue 1409): before this, the only limit was a timer-driven sweep,
// letting a distinct-hash flood balloon memory between sweeps and making the
// sweep's own full-map sort scale with the flood. New keys are rejected once
// the map is full (the same inline-cap discipline as the peer registry's
// lastAsserted map); updates to existing keys and deletes are always allowed,
// and the periodic sweep keeps freeing space by TTL so attribution recovers
// after a flood subsides.
type cappedPeerMap struct {
	mu      sync.Mutex
	entries map[string]peerMapEntry
	maxSize int

	// rejected counts inserts refused because the map was full, since the
	// last sweep reset — flood observability without a per-insert log line.
	rejected atomic.Int64
}

// setMaxSize configures the insert cap. The zero value (no cap configured)
// stores without bound, preserving the zero-value usability that dozens of
// test fixtures rely on; Server.New always configures a positive cap.
func (m *cappedPeerMap) setMaxSize(maxSize int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.maxSize = maxSize
}

// Store inserts or updates an entry. A new key is rejected (returning false)
// when the map is at capacity; updating an existing key always succeeds.
func (m *cappedPeerMap) Store(hash string, entry peerMapEntry) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.entries == nil {
		m.entries = make(map[string]peerMapEntry)
	}

	if _, exists := m.entries[hash]; !exists && m.maxSize > 0 && len(m.entries) >= m.maxSize {
		m.rejected.Add(1)
		return false
	}

	m.entries[hash] = entry

	return true
}

// Load returns the entry for hash, if present.
func (m *cappedPeerMap) Load(hash string) (peerMapEntry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.entries[hash]

	return entry, ok
}

// Delete removes the entry for hash, if present.
func (m *cappedPeerMap) Delete(hash string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.entries, hash)
}

// Clear removes every entry.
func (m *cappedPeerMap) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.entries = make(map[string]peerMapEntry)
}

// Len returns the number of entries.
func (m *cappedPeerMap) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.entries)
}

// Range calls f for each entry over a snapshot of the map, so f may call
// Delete without deadlocking. Iteration order is unspecified.
func (m *cappedPeerMap) Range(f func(hash string, entry peerMapEntry) bool) {
	m.mu.Lock()
	snapshot := make(map[string]peerMapEntry, len(m.entries))
	for k, v := range m.entries {
		snapshot[k] = v
	}
	m.mu.Unlock()

	for k, v := range snapshot {
		if !f(k, v) {
			return
		}
	}
}

// RejectedSinceLastRead returns the number of full-map insert rejections since
// the previous call, resetting the counter.
func (m *cappedPeerMap) RejectedSinceLastRead() int64 {
	return m.rejected.Swap(0)
}
