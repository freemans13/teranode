package p2p

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// cappedPeerMap is a size-bounded map from announced hash to the peer that
// announced it (ban attribution). Gossip inserts are driven entirely by
// untrusted input — an announcement stores the peer BEFORE any check that the
// hash names a real block or subtree — so the bound is enforced inline at
// insert (issue 1409): before this, the only limit was a timer-driven sweep,
// letting a distinct-hash flood balloon memory between sweeps and making the
// sweep's own full-map sort scale with the flood.
//
// At capacity a new key evicts the OLDEST entry rather than being refused.
// That direction is load-bearing for security, not a style choice: refusing
// the new key would let a flooder fill every slot with junk hashes and thereby
// suppress attribution for the invalid block it announces next — switching off
// the node's only automatic ban path for invalid blocks. Evicting oldest keeps
// memory bounded while always retaining the most recent announcements, which
// are the ones an in-flight validation is about to report. It is also the
// behaviour p2p_peer_map_max_size documents.
//
// Eviction is O(1): a list holds keys in insertion order (most recent at the
// back), so no sort is needed at insert or at sweep time.
type cappedPeerMap struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	order   *list.List // front = oldest, back = newest; values are *peerMapNode
	maxSize int

	// evicted counts entries dropped to make room since the last sweep read —
	// flood observability without a per-insert log line.
	evicted atomic.Int64
}

// peerMapNode is the list payload: the key plus its entry, so evicting from
// the list front also identifies the map key to delete.
type peerMapNode struct {
	hash  string
	entry peerMapEntry
}

// setMaxSize configures the insert cap. The zero value (no cap configured)
// stores without bound, preserving the zero-value usability that many test
// fixtures rely on; NewServer always configures a positive cap.
func (m *cappedPeerMap) setMaxSize(maxSize int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.maxSize = maxSize
}

// init prepares the internal structures; callers must hold the mutex.
func (m *cappedPeerMap) initLocked() {
	if m.entries == nil {
		m.entries = make(map[string]*list.Element)
		m.order = list.New()
	}
}

// Store inserts or updates an entry, evicting the oldest entry first when a
// new key would exceed the cap. Updating an existing key refreshes its value
// and its recency.
func (m *cappedPeerMap) Store(hash string, entry peerMapEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.initLocked()

	if existing, ok := m.entries[hash]; ok {
		existing.Value.(*peerMapNode).entry = entry
		m.order.MoveToBack(existing)

		return
	}

	if m.maxSize > 0 && m.order.Len() >= m.maxSize {
		if oldest := m.order.Front(); oldest != nil {
			m.order.Remove(oldest)
			delete(m.entries, oldest.Value.(*peerMapNode).hash)
			m.evicted.Add(1)
		}
	}

	m.entries[hash] = m.order.PushBack(&peerMapNode{hash: hash, entry: entry})
}

// Load returns the entry for hash, if present. It does not change recency:
// eviction order tracks when a hash was announced, not when it was looked up.
func (m *cappedPeerMap) Load(hash string) (peerMapEntry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	element, ok := m.entries[hash]
	if !ok {
		return peerMapEntry{}, false
	}

	return element.Value.(*peerMapNode).entry, true
}

// Delete removes the entry for hash, if present.
func (m *cappedPeerMap) Delete(hash string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if element, ok := m.entries[hash]; ok {
		m.order.Remove(element)
		delete(m.entries, hash)
	}
}

// Clear removes every entry.
func (m *cappedPeerMap) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.entries = nil
	m.order = nil
}

// Len returns the number of entries.
func (m *cappedPeerMap) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.entries)
}

// Range calls f for each entry over a snapshot of the keys, so f may call
// Delete without deadlocking. Iteration runs oldest-first.
func (m *cappedPeerMap) Range(f func(hash string, entry peerMapEntry) bool) {
	m.mu.Lock()

	snapshot := make([]peerMapNode, 0, len(m.entries))
	if m.order != nil {
		for element := m.order.Front(); element != nil; element = element.Next() {
			snapshot = append(snapshot, *element.Value.(*peerMapNode))
		}
	}

	m.mu.Unlock()

	for _, node := range snapshot {
		if !f(node.hash, node.entry) {
			return
		}
	}
}

// EvictedSinceLastRead returns the number of entries dropped to make room
// since the previous call, resetting the counter.
func (m *cappedPeerMap) EvictedSinceLastRead() int64 {
	return m.evicted.Swap(0)
}
