package adaptivefetch

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNew_ReturnsPessimisticByDefault(t *testing.T) {
	s, err := New(Config{
		WindowSize:                10,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeAuto,
	}, "test")
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
	}, "test")
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
	}, "test")
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
			_, err := New(c, "test")
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
	}, "test")
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
	}, "test")
	require.NoError(t, err)

	s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, Mode: ModePessimistic})
	s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, Mode: ModePessimistic})
	s.Record(Observation{TotalTxs: 1000, LocalHits: 950, Mode: ModePessimistic})
	require.Equal(t, ModePessimistic, s.Mode())
}
