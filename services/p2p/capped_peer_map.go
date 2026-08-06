package p2p

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
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
// That direction is deliberate: refusing the new key would let a flooder fill
// every slot with junk hashes ahead of time and so suppress attribution for
// the invalid block it announces next. Evicting oldest keeps memory bounded
// while always retaining the most recent announcements, which are the ones an
// in-flight validation is about to report.
//
// What this does NOT do is make attribution flood-proof. The map is a single
// space shared by all peers, so a peer that announces enough distinct hashes
// AFTER an honest announcement can still age that honest entry out before the
// block finishes validating. Evicting oldest changes when a flooder can strike,
// not whether it can; closing that needs a per-peer share of the map, tracked
// in issue 1503. Attribution is best-effort here, as the TTL already implies.
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
// and its recency, so attribution for a hash announced by two peers is
// last-writer-wins — unchanged from the sync.Map this replaced, and reachable
// only when a second node genuinely announces the same hash as its own tip,
// since the fromID check rejects re-attribution by relays.
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

// DeleteExpired removes every entry whose timestamp predates cutoff and
// returns how many it removed. This is the TTL sweep's one pass over the map:
// it holds the lock once and allocates nothing, where Range would copy every
// entry into a snapshot first and the caller would then re-enter the lock per
// deletion.
//
// It walks the whole list rather than stopping at the first live entry.
// Insertion order tracks timestamp order closely but not strictly — two
// concurrent announcements can read the clock in one order and take the lock
// in the other — so an early exit could skip an expired entry and leave it for
// the next sweep. At the configured cap the full walk costs little enough that
// the exactness is worth more than the saved iterations.
func (m *cappedPeerMap) DeleteExpired(cutoff time.Time) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.order == nil {
		return 0
	}

	removed := 0

	for element := m.order.Front(); element != nil; {
		next := element.Next()

		if node := element.Value.(*peerMapNode); node.entry.timestamp.Before(cutoff) {
			m.order.Remove(element)
			delete(m.entries, node.hash)

			removed++
		}

		element = next
	}

	return removed
}

// EvictedSinceLastRead returns the number of entries dropped to make room
// since the previous call, resetting the counter.
func (m *cappedPeerMap) EvictedSinceLastRead() int64 {
	return m.evicted.Swap(0)
}
