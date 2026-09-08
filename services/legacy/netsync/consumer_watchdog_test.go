package netsync

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// captureLogger records Warnf lines so a test can assert on what the watchdog
// said. It implements only what the watchdog uses; every other method would be
// dead weight in a test whose subject is one log line.
type captureLogger struct {
	ulogger.Logger

	warns []string
}

func (c *captureLogger) Warnf(format string, args ...interface{}) {
	c.warns = append(c.warns, fmt.Sprintf(format, args...))
}

func TestReportConsumerStallStaysQuietBeforeTheThreshold(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}
	sm := &SyncManager{logger: log}

	now := time.Now()

	sm.noteConsumerAdmitted(now)
	sm.publishConsumerWait(now, true, nil)

	// One second short of the threshold. A loop that has just admitted a block is
	// not stuck, and a watchdog that says so on every tick is noise.
	sm.reportConsumerStall(now.Add(consumerStallAfter - time.Second))

	require.Empty(t, log.warns)
}

func TestReportConsumerStallNamesTheBlockHoldingTheQueueArmShut(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}

	hash := chainhash.HashH([]byte("pending"))

	sm := &SyncManager{
		logger:     log,
		dispatcher: &blockDispatcher{budget: 4096},
	}

	now := time.Now()

	sm.noteConsumerAdmitted(now)

	// The wedge this exists for: a head-processed block the window will not take,
	// so the queue arm is shut and nothing else is head-processed behind it.
	sm.publishConsumerWait(now, false, &blockDispatch{
		msg:      &blockQueueMsg{blockHash: hash},
		height:   754896,
		windowed: true,
	})

	sm.reportConsumerStall(now.Add(consumerStallAfter + time.Second))

	require.Len(t, log.warns, 1)

	line := log.warns[0]

	require.Contains(t, line, "no block admitted")
	require.Contains(t, line, "queue arm shut")
	require.Contains(t, line, hash.String()[:8])
	require.Contains(t, line, "754896")
	require.Contains(t, line, "the window is empty")
}

func TestReportConsumerStallReportsOnceAnInterval(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}
	sm := &SyncManager{logger: log}

	now := time.Now()

	sm.noteConsumerAdmitted(now)
	sm.publishConsumerWait(now, true, nil)

	stalled := now.Add(consumerStallAfter + time.Second)

	sm.reportConsumerStall(stalled)
	sm.reportConsumerStall(stalled.Add(time.Second))
	sm.reportConsumerStall(stalled.Add(consumerStallReportInterval))

	// Twice, not three times: the middle call is inside the interval. A wedge
	// lasting hours must not fill the disk with the same sentence.
	require.Len(t, log.warns, 2)
}

func TestReportConsumerStallSaysWhenTheLoopNeverReachedItsWait(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}
	sm := &SyncManager{logger: log}

	now := time.Now()

	sm.noteConsumerAdmitted(now)

	// No snapshot published: the loop is stuck in a block's own work above the
	// select rather than waiting for capacity, which is a different fault and
	// must not be reported as the same one.
	sm.reportConsumerStall(now.Add(consumerStallAfter + time.Second))

	require.Len(t, log.warns, 1)
	require.Contains(t, log.warns[0], "has not reached its wait")
}

func TestReportConsumerStallStaysQuietUntilSomethingIsAdmitted(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}
	sm := &SyncManager{logger: log}

	// The clock was never started, which is the state of a manager whose loop has
	// not run. There is no silence to measure, so there is nothing to say.
	sm.reportConsumerStall(time.Now().Add(time.Hour))

	require.Empty(t, log.warns)
}

func TestPublishConsumerWaitDescribesTheWindowAndTheBarrier(t *testing.T) {
	settled := make(chan struct{})
	close(settled)

	entry := &frontierEntry{
		hash:    chainhash.HashH([]byte("settled entry")),
		height:  754895,
		settled: settled,
	}
	entry.failed.Store(true)

	running := &frontierEntry{
		hash:    chainhash.HashH([]byte("running entry")),
		height:  754896,
		settled: make(chan struct{}),
	}

	log := &captureLogger{Logger: ulogger.TestLogger{}}

	sm := &SyncManager{
		logger: log,
		dispatcher: &blockDispatcher{
			barrier:  true,
			budget:   8192,
			inflight: 2048,
			frontier: []*frontierEntry{entry, running},
		},
	}

	now := time.Now()

	sm.noteConsumerAdmitted(now)
	sm.publishConsumerWait(now, false, nil)
	sm.reportConsumerStall(now.Add(consumerStallAfter + time.Second))

	require.Len(t, log.warns, 1)

	line := log.warns[0]

	// A settled entry still in the window is a completion that was never
	// processed, and a running one with nothing alive to finish it is a
	// completion that was lost. The report has to tell them apart.
	require.Contains(t, line, "settled")
	require.Contains(t, line, "failed")
	require.Contains(t, line, "running")
	require.Contains(t, line, "checkpoint barrier is set")
	require.Contains(t, line, "2048 of 8192 window bytes charged")
}

func TestPublishConsumerWaitIsSafeToReadFromAnotherGoroutine(t *testing.T) {
	sm := &SyncManager{
		logger:     &captureLogger{Logger: ulogger.TestLogger{}},
		dispatcher: &blockDispatcher{budget: 1024},
	}

	sm.noteConsumerAdmitted(time.Now())

	var stop atomic.Bool

	done := make(chan struct{})

	// The consumer's side: publish repeatedly, as the loop does once per turn.
	go func() {
		defer close(done)

		for i := 0; !stop.Load(); i++ {
			sm.publishConsumerWait(time.Now(), i%2 == 0, nil)
		}
	}()

	// The watchdog's side. Under -race this is the assertion: the snapshot is
	// handed over through an atomic value, so the goroutine whose progress is in
	// question never waits on the goroutine describing it.
	for i := 0; i < 2000; i++ {
		sm.reportConsumerStall(time.Now().Add(time.Hour))
	}

	stop.Store(true)
	<-done
}

func TestDescribeFrontierEntryHandlesANilSlot(t *testing.T) {
	// complete clears a popped slot to nil before resliding, so a snapshot taken
	// mid-pop can see one. Printing "nil" beats panicking inside a watchdog.
	require.Equal(t, "nil", describeFrontierEntry(nil))
}

func TestShortHashLeavesAShortStringAlone(t *testing.T) {
	require.Equal(t, "abc", shortHash("abc"))
	require.Equal(t, "0123456789"[:8], shortHash("0123456789"))
}

func TestConsumerStallLineIsOneLine(t *testing.T) {
	log := &captureLogger{Logger: ulogger.TestLogger{}}

	sm := &SyncManager{
		logger:     log,
		dispatcher: &blockDispatcher{budget: 1},
	}

	now := time.Now()

	sm.noteConsumerAdmitted(now)
	sm.publishConsumerWait(now, false, nil)
	sm.reportConsumerStall(now.Add(consumerStallAfter + time.Second))

	require.Len(t, log.warns, 1)

	// The project's logging convention: one line per message, so a grep for the
	// tag returns the whole finding rather than its first clause.
	require.NotContains(t, log.warns[0], "\n")
	require.False(t, strings.HasSuffix(log.warns[0], " "))
}
