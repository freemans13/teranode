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
