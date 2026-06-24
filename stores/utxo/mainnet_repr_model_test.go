package utxo_test

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOutCountSampler_MatchesMainnetShape(t *testing.T) {
	s := newOutCountSampler(42)
	const n = 200000
	vals := make([]int, n)
	var sum int
	for i := range vals {
		v := s.sample()
		require.GreaterOrEqual(t, v, 1)
		require.LessOrEqual(t, v, 1024) // capped
		vals[i] = v
		sum += v
	}
	sort.Ints(vals)
	median := vals[n/2]
	p90 := vals[n*90/100]
	p99 := vals[n*99/100]
	mean := float64(sum) / n
	require.Equal(t, 2, median, "median outputs")
	require.LessOrEqual(t, p90, 15, "p90")
	require.InDelta(t, 139, p99, 30, "p99")
	require.InDelta(t, 10.0, mean, 4.0, "mean ~10")
}

// outCountSampler draws outputs-per-tx calibrated to mainnet txs_p07
// (median 2, mean ~10, p90 13, p99 139), capped at 1024 to bound memory.
type outCountSampler struct{ rng *rand.Rand }

func newOutCountSampler(seed int64) *outCountSampler {
	return &outCountSampler{rng: rand.New(rand.NewSource(seed))}
}

func (s *outCountSampler) sample() int {
	u := s.rng.Float64()
	switch {
	case u < 0.75:
		return 1 + s.rng.Intn(2) // 1-2  (median 2)
	case u < 0.92:
		return 3 + s.rng.Intn(8) // 3-10 (p90 ~10)
	case u < 0.99:
		return 11 + s.rng.Intn(129) // 11-139 (p99 ~139)
	default:
		return 140 + s.rng.Intn(885) // 140-1024 tail
	}
}

func TestSpendAgeSampler_MatchesMainnetShape(t *testing.T) {
	s := newSpendAgeSampler(7)
	const n = 200000
	vals := make([]int, n)
	var sameBlock, within6 int
	for i := range vals {
		v := s.sample()
		require.GreaterOrEqual(t, v, 0)
		if v == 0 {
			sameBlock++
		}
		if v <= 6 {
			within6++
		}
		vals[i] = v
	}
	sort.Ints(vals)
	p50, p90 := vals[n/2], vals[n*90/100]
	require.InDelta(t, 0.10, float64(sameBlock)/n, 0.03, "same-block frac")
	require.InDelta(t, 0.25, float64(within6)/n, 0.04, "<=6 frac")
	require.InDelta(t, 215, p50, 60, "p50")
	require.InDelta(t, 6273, p90, 1200, "p90")
}

// spendAgeSampler draws heights-until-spent calibrated to mainnet
// (same-block 10%, <=6 25%, p50 215, p90 6273, p99 47872, max ~154570).
type spendAgeSampler struct{ rng *rand.Rand }

func newSpendAgeSampler(seed int64) *spendAgeSampler {
	return &spendAgeSampler{rng: rand.New(rand.NewSource(seed))}
}

func (s *spendAgeSampler) sample() int {
	u := s.rng.Float64()
	switch {
	case u < 0.10:
		return 0
	case u < 0.25:
		return 1 + s.rng.Intn(6) // 1-6
	case u < 0.50:
		return 7 + s.rng.Intn(209) // 7-215
	case u < 0.90:
		return 216 + s.rng.Intn(6058) // 216-6273
	case u < 0.99:
		return 6274 + s.rng.Intn(41599) // 6274-47872
	default:
		return 47873 + s.rng.Intn(106698) // tail to ~154570
	}
}
