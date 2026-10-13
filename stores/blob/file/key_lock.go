package file

import "sync"

// blobKeyLocks serialises, per blob, the steps that decide its delete-at-height: an exclusive
// publish (its schedule and the publish itself) and SetDAH. It is per process and keyed by the
// blob's file name, so every File instance over the same directory shares it.
//
// The scheduler that holds delete-at-heights is last-writer-wins, and nothing in it knows
// whether the blob is on disk. So the order in which a publish and a SetDAH reach it is the
// whole answer, and it is decided here. Under this lock:
//
//   - an exclusive publish checks the name is free, schedules the blob's deletion, and then
//     publishes it, so a blob is never visible without its delete-at-height already scheduled;
//   - a publish that finds the name taken schedules nothing;
//   - SetDAH changes the delete-at-height of a blob only once no exclusive publish of it is in
//     flight.
//
// A promotion (SetDAH 0) can therefore only follow the publish's schedule, never precede it,
// and it is never undone by the publish it overlapped. A plain write (no exclusive publish) is
// not covered: it schedules before it publishes and takes no lock, as it always has, and a
// plain write over an existing blob still replaces that blob's delete-at-height.
//
// One mutex per name in use, created on demand and dropped when its last holder leaves, so a
// slow scheduler call on one blob never holds up another.
var blobKeyLocks = keyLocks{locks: make(map[string]*keyLock)}

type keyLocks struct {
	mu    sync.Mutex
	locks map[string]*keyLock
}

type keyLock struct {
	mu   sync.Mutex
	refs int
}

// lock takes name's lock and returns the function that gives it back.
func (k *keyLocks) lock(name string) func() {
	k.mu.Lock()

	l, ok := k.locks[name]
	if !ok {
		l = &keyLock{}
		k.locks[name] = l
	}

	l.refs++
	k.mu.Unlock()

	l.mu.Lock()

	return func() {
		l.mu.Unlock()

		k.mu.Lock()

		l.refs--
		if l.refs == 0 {
			delete(k.locks, name)
		}

		k.mu.Unlock()
	}
}
