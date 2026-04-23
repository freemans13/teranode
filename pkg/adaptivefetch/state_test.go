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
		s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, MissingFetches: 0, Mode: ModePessimistic})
		require.Equal(t, ModePessimistic, s.Mode(), "block %d: window not full, must stay pessimistic", i)
	}

	s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, MissingFetches: 0, Mode: ModePessimistic})
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

	s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, Mode: ModePessimistic})
	s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, Mode: ModePessimistic})
	s.Record(Observation{TotalTxs: 1000, LocalHits: 950, Mode: ModePessimistic})
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

	s.Record(Observation{TotalTxs: 10000, LocalHits: 9800, MissingFetches: 200, Mode: ModeOptimistic})
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

	s.Record(Observation{TotalTxs: 10000, LocalHits: 9950, MissingFetches: 50, Mode: ModeOptimistic})
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
		s.Record(Observation{TotalTxs: 10000, LocalHits: 9980, MissingFetches: 20, Mode: ModeOptimistic})
		require.Equal(t, ModeOptimistic, s.Mode(), "block %d: window not full yet", i)
	}
	s.Record(Observation{TotalTxs: 10000, LocalHits: 9980, MissingFetches: 20, Mode: ModeOptimistic})
	require.Equal(t, ModePessimistic, s.Mode())
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
				s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, Mode: ModePessimistic})
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
	s.Record(Observation{TotalTxs: 0, LocalHits: 0, Mode: ModePessimistic})
	s.Record(Observation{TotalTxs: -5, LocalHits: 10, Mode: ModePessimistic})
	s.Record(Observation{TotalTxs: 100, LocalHits: -1, Mode: ModePessimistic})
	s.Record(Observation{TotalTxs: 100, LocalHits: 200, Mode: ModePessimistic}) // LocalHits > TotalTxs
	s.Record(Observation{TotalTxs: 100, LocalHits: 50, MissingFetches: -1, Mode: ModePessimistic})

	require.Equal(t, ModePessimistic, s.Mode(), "invalid observations must not alter window")

	// Now 3 valid perfect observations must be enough to flip Pess→Opt (WindowSize=3).
	s.Record(Observation{TotalTxs: 100, LocalHits: 100, Mode: ModePessimistic})
	s.Record(Observation{TotalTxs: 100, LocalHits: 100, Mode: ModePessimistic})
	s.Record(Observation{TotalTxs: 100, LocalHits: 100, Mode: ModePessimistic})
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
		s.Record(Observation{TotalTxs: 100, LocalHits: 100, MissingFetches: 0, Mode: ModeOptimistic})
	}
	require.Equal(t, ModeOptimistic, s.Mode())
}

// TestNoWallClockOrFSMDependency pins the design invariant that the gate
// is NOT driven by FSM state or wall-clock time. If a future edit imports
// blockchain_api for FSM checks or time for age-based logic inside this
// package, this test's grep-style check fails and forces a review.
//
// Rationale: PR #598 was reverted via PR #647 because clock/FSM gating
// cascaded under load. The adaptive-fetch design deliberately avoids
// that whole class of bug by driving transitions solely from counts.
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
