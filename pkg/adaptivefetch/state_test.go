package adaptivefetch

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestNew_ReturnsPessimisticByDefault(t *testing.T) {
	s, err := New(Config{
		WindowSize:                10,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeAuto,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)
	require.Equal(t, ModePessimistic, s.Mode())
	require.False(t, s.ShouldSkipSubtreeData())
}

func TestNew_BootstrapOptimistic_StartsOptimistic(t *testing.T) {
	s, err := New(Config{
		WindowSize:                10,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeOptimistic,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)
	require.Equal(t, ModeOptimistic, s.Mode())
	require.True(t, s.ShouldSkipSubtreeData())
}

func TestNew_BootstrapAuto_ResolvesToPessimistic(t *testing.T) {
	s, err := New(Config{
		WindowSize:                10,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeAuto,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)
	require.Equal(t, ModePessimistic, s.Mode())
}

func TestNew_RejectsInvalidConfig(t *testing.T) {
	base := Config{
		WindowSize:                10,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeAuto,
	}

	cases := []struct {
		name   string
		mutate func(*Config)
		needle string
	}{
		{"zero window", func(c *Config) { c.WindowSize = 0 }, "WindowSize"},
		{"negative window", func(c *Config) { c.WindowSize = -1 }, "WindowSize"},
		{"hit rate below zero", func(c *Config) { c.PessToOptHitRateThreshold = -0.1 }, "PessToOptHitRateThreshold"},
		{"hit rate above one", func(c *Config) { c.PessToOptHitRateThreshold = 1.1 }, "PessToOptHitRateThreshold"},
		{"negative miss threshold", func(c *Config) { c.OptToPessMissThreshold = -1 }, "OptToPessMissThreshold"},
		{"negative avg miss threshold", func(c *Config) { c.OptToPessAvgMissThreshold = -1 }, "OptToPessAvgMissThreshold"},
		{"invalid bootstrap mode", func(c *Config) { c.BootstrapMode = Mode(99) }, "BootstrapMode"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.mutate(&c)
			_, err := New(c, "test", prometheus.NewRegistry())
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.needle)
		})
	}
}

func TestRecord_PessToOpt_HighHitRateFullWindow(t *testing.T) {
	s, err := New(Config{
		WindowSize:                5,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModePessimistic,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, MissingFetches: 0})
		require.Equal(t, ModePessimistic, s.Mode(), "block %d: window not full, must stay pessimistic", i)
	}

	s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, MissingFetches: 0})
	require.Equal(t, ModeOptimistic, s.Mode())
}

func TestRecord_PessStays_WhenHitRateBelowThreshold(t *testing.T) {
	s, err := New(Config{
		WindowSize:                3,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModePessimistic,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)

	s.Record(Observation{TotalTxs: 1000, LocalHits: 1000})
	s.Record(Observation{TotalTxs: 1000, LocalHits: 1000})
	s.Record(Observation{TotalTxs: 1000, LocalHits: 950})
	require.Equal(t, ModePessimistic, s.Mode())
}

func TestRecord_OptToPess_SingleBadBlockTrips(t *testing.T) {
	s, err := New(Config{
		WindowSize:                10,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeOptimistic,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)
	require.Equal(t, ModeOptimistic, s.Mode())

	s.Record(Observation{TotalTxs: 10000, LocalHits: 9800, MissingFetches: 200})
	require.Equal(t, ModePessimistic, s.Mode(), "single block with 200 misses must trip immediately")
}

func TestRecord_OptStays_WhenMissesBelowSingleBlockThreshold(t *testing.T) {
	s, err := New(Config{
		WindowSize:                10,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeOptimistic,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)

	s.Record(Observation{TotalTxs: 10000, LocalHits: 9950, MissingFetches: 50})
	require.Equal(t, ModeOptimistic, s.Mode())
}

func TestRecord_OptToPess_RollingAverageTrip(t *testing.T) {
	s, err := New(Config{
		WindowSize:                5,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeOptimistic,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		s.Record(Observation{TotalTxs: 10000, LocalHits: 9980, MissingFetches: 20})
		require.Equal(t, ModeOptimistic, s.Mode(), "block %d: window not full yet", i)
	}
	s.Record(Observation{TotalTxs: 10000, LocalHits: 9980, MissingFetches: 20})
	require.Equal(t, ModePessimistic, s.Mode())
}

// TestRecord_OptToPess_ThresholdBoundaryIsInclusive locks in the documented
// inclusive (>=) threshold semantics for both Opt→Pess trips. A misses count
// or rolling-average exactly equal to the configured threshold MUST trip back
// to pessimistic; otherwise a misconfigured node could sit at threshold-value
// forever without ever recovering. Regression guard for review-round-2.
func TestRecord_OptToPess_ThresholdBoundaryIsInclusive(t *testing.T) {
	t.Run("single-block boundary trips", func(t *testing.T) {
		s, err := New(Config{
			WindowSize:                10,
			PessToOptHitRateThreshold: 0.99,
			OptToPessMissThreshold:    100,
			OptToPessAvgMissThreshold: 1000, // never trips on average
			BootstrapMode:             ModeOptimistic,
		}, "test", prometheus.NewRegistry())
		require.NoError(t, err)

		// MissingFetches == OptToPessMissThreshold MUST trip (inclusive).
		s.Record(Observation{TotalTxs: 10000, LocalHits: 9900, MissingFetches: 100})
		require.Equal(t, ModePessimistic, s.Mode(),
			"misses == single-block threshold must trip (inclusive)")
	})

	t.Run("rolling-average boundary trips", func(t *testing.T) {
		s, err := New(Config{
			WindowSize:                5,
			PessToOptHitRateThreshold: 0.99,
			OptToPessMissThreshold:    1000, // never trips on single block
			OptToPessAvgMissThreshold: 10,
			BootstrapMode:             ModeOptimistic,
		}, "test", prometheus.NewRegistry())
		require.NoError(t, err)

		// 5 observations of 10 misses each → average exactly 10 → MUST trip.
		for i := 0; i < 4; i++ {
			s.Record(Observation{TotalTxs: 10000, LocalHits: 9990, MissingFetches: 10})
			require.Equal(t, ModeOptimistic, s.Mode(), "block %d: window not full yet", i)
		}
		s.Record(Observation{TotalTxs: 10000, LocalHits: 9990, MissingFetches: 10})
		require.Equal(t, ModePessimistic, s.Mode(),
			"avg-misses == avg-threshold must trip (inclusive)")
	})
}

func TestRecord_ConcurrentIsRaceClean(t *testing.T) {
	s, err := New(Config{
		WindowSize:                64,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    1000,
		OptToPessAvgMissThreshold: 100,
		BootstrapMode:             ModePessimistic,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)

	const goroutines = 16
	const perGoroutine = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				s.Record(Observation{TotalTxs: 1000, LocalHits: 1000})
				_ = s.ShouldSkipSubtreeData()
				_ = s.Mode()
			}
		}()
	}
	wg.Wait()

	require.Equal(t, ModeOptimistic, s.Mode())
}

func TestRecord_IgnoresInvalidObservations(t *testing.T) {
	s, err := New(Config{
		WindowSize:                3,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModePessimistic,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)

	// Each of these should be silently dropped — window should stay empty,
	// so a subsequent Pess→Opt should not fire until 3 VALID observations arrive.
	s.Record(Observation{TotalTxs: 0, LocalHits: 0})
	s.Record(Observation{TotalTxs: -5, LocalHits: 10})
	s.Record(Observation{TotalTxs: 100, LocalHits: -1})
	s.Record(Observation{TotalTxs: 100, LocalHits: 200}) // LocalHits > TotalTxs
	s.Record(Observation{TotalTxs: 100, LocalHits: 50, MissingFetches: -1})

	require.Equal(t, ModePessimistic, s.Mode(), "invalid observations must not alter window")

	// Now 3 valid perfect observations must be enough to flip Pess→Opt (WindowSize=3).
	s.Record(Observation{TotalTxs: 100, LocalHits: 100})
	s.Record(Observation{TotalTxs: 100, LocalHits: 100})
	s.Record(Observation{TotalTxs: 100, LocalHits: 100})
	require.Equal(t, ModeOptimistic, s.Mode())
}

func TestRecord_RingBufferWraparound(t *testing.T) {
	s, err := New(Config{
		WindowSize:                3,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    1000, // never triggers
		OptToPessAvgMissThreshold: 1000, // never triggers
		BootstrapMode:             ModeOptimistic,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)

	// Write 2×WindowSize observations to force wraparound. Mode should stay optimistic
	// because all observations are clean.
	for i := 0; i < 6; i++ {
		s.Record(Observation{TotalTxs: 100, LocalHits: 100, MissingFetches: 0})
	}
	require.Equal(t, ModeOptimistic, s.Mode())
}

// TestRecordIfMode_DropsCrossModeObservation verifies that an observation
// tagged with one mode is discarded when the live mode has transitioned
// to the other before Record is called. This is the explicit guard against
// concurrent workers writing observations from a previous mode into the
// current mode's rolling window.
func TestRecordIfMode_DropsCrossModeObservation(t *testing.T) {
	s, err := New(Config{
		WindowSize:                3,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModePessimistic,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)

	// Fill window with valid pessimistic observations so the next perfect
	// pessimistic observation will flip the state to optimistic.
	for i := 0; i < 3; i++ {
		s.RecordIfMode(ModePessimistic, Observation{TotalTxs: 100, LocalHits: 100})
	}
	require.Equal(t, ModeOptimistic, s.Mode(),
		"three perfect pessimistic samples must trip Pess→Opt")

	// Now the live mode is optimistic. A late observation tagged as
	// pessimistic (e.g. recorded by a worker that started before the
	// transition) must be dropped — otherwise it would contaminate the
	// optimistic-mode window with synthetic LocalHits that bypass the
	// real OptToPess gates.
	s.RecordIfMode(ModePessimistic, Observation{TotalTxs: 1000, LocalHits: 1000, MissingFetches: 0})
	require.Equal(t, ModeOptimistic, s.Mode(),
		"cross-mode observation must not alter mode")

	// Conversely, an observation tagged with the live mode is recorded.
	// A miss large enough to trip the immediate OptToPess threshold proves
	// the observation actually entered the window.
	s.RecordIfMode(ModeOptimistic, Observation{TotalTxs: 1000, LocalHits: 800, MissingFetches: 200})
	require.Equal(t, ModePessimistic, s.Mode(),
		"matching-mode observation with MissingFetches above threshold must trip Opt→Pess")
}

// TestRecordIfMode_NilReceiver locks in the nil-safe contract.
func TestRecordIfMode_NilReceiver(t *testing.T) {
	var s *State
	require.NotPanics(t, func() {
		s.RecordIfMode(ModePessimistic, Observation{TotalTxs: 100, LocalHits: 100})
	})
}

// TestParseBootstrapMode covers ParseBootstrapMode's accepted input set,
// case-insensitivity, the empty-string-as-auto convention, and the
// error path for unknown values.
func TestParseBootstrapMode(t *testing.T) {
	cases := []struct {
		in   string
		want Mode
		err  bool
	}{
		{"pessimistic", ModePessimistic, false},
		{"optimistic", ModeOptimistic, false},
		{"auto", ModeAuto, false},
		{"", ModeAuto, false},
		{"Optimistic", ModeOptimistic, false},
		{"nonsense", ModeAuto, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseBootstrapMode(tc.in)
			if tc.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestNoWallClockOrFSMDependency pins the design invariant that the gate
// is NOT driven by FSM state or wall-clock time. If a future edit imports
// blockchain_api for FSM checks or time for age-based logic inside this
// package, this test's grep-style check fails and forces a review.
//
// Rationale: PR #598 was reverted via PR #647 because clock/FSM gating
// cascaded under load. The adaptive-fetch design deliberately avoids
// that whole class of bug by driving transitions solely from counts.
func TestNoWallClockOrFSMDependency(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)

	forbidden := []string{
		`"time"`,
		"blockchain_api",
		"FSMStateType",
		"time.Now",
		"time.Since",
	}

	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		data, err := os.ReadFile(f)
		require.NoError(t, err)
		src := string(data)
		for _, needle := range forbidden {
			require.NotContainsf(t, src, needle,
				"adaptivefetch package must not reference %q (found in %s). "+
					"See TestNoWallClockOrFSMDependency docstring for why.", needle, f)
		}
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	require.NoError(t, cfg.Validate(), "DefaultConfig must pass Validate")

	// Pin the semantics — changing these values is a behaviour change that
	// should be reviewed explicitly. Not a locked contract, but a speed bump.
	require.Equal(t, 10, cfg.WindowSize)
	require.InDelta(t, 0.99, cfg.PessToOptHitRateThreshold, 0.0001)
	require.Equal(t, 100, cfg.OptToPessMissThreshold)
	require.InDelta(t, 10.0, cfg.OptToPessAvgMissThreshold, 0.0001)
	require.Equal(t, ModeAuto, cfg.BootstrapMode)
}
