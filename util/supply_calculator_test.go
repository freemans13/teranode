package util

import (
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
)

func TestCalculateExpectedSupplyAtHeight(t *testing.T) {
	tests := []struct {
		name           string
		height         uint32
		expectedSupply uint64
	}{
		{
			name:           "Genesis block",
			height:         0,
			expectedSupply: 5000000000, // 50 BTC
		},
		{
			name:           "Block 1",
			height:         1,
			expectedSupply: 10000000000, // 100 BTC (2 blocks * 50 BTC each)
		},
		{
			name:           "Last block of first halving period",
			height:         209999,
			expectedSupply: 210000 * 5000000000, // 210,000 blocks * 50 BTC each
		},
		{
			name:           "First block of second halving period",
			height:         210000,
			expectedSupply: (210000 * 5000000000) + 2500000000, // First period + first block of second period
		},
		{
			name:           "End of second halving period",
			height:         419999,
			expectedSupply: (210000 * 5000000000) + (210000 * 2500000000), // Two full periods
		},
		{
			name:           "First block of third halving period",
			height:         420000,
			expectedSupply: (210000 * 5000000000) + (210000 * 2500000000) + 1250000000, // Two periods + first block of third
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			supply := CalculateExpectedSupplyAtHeight(tt.height, &chaincfg.MainNetParams)
			if supply != tt.expectedSupply {
				t.Errorf("CalculateExpectedSupplyAtHeight(%d) = %d, want %d",
					tt.height, supply, tt.expectedSupply)
			}
		})
	}
}

func TestCalculateExpectedSupplyAtHeight_EdgeCases(t *testing.T) {
	t.Run("Nil params", func(t *testing.T) {
		supply := CalculateExpectedSupplyAtHeight(100, nil)
		if supply != 0 {
			t.Errorf("Expected 0 for nil params, got %d", supply)
		}
	})

	t.Run("Zero subsidy reduction interval", func(t *testing.T) {
		params := &chaincfg.Params{SubsidyReductionInterval: 0}
		supply := CalculateExpectedSupplyAtHeight(100, params)
		if supply != 0 {
			t.Errorf("Expected 0 for zero interval, got %d", supply)
		}
	})

	t.Run("Very high block height (beyond all halvings)", func(t *testing.T) {
		// At height 6930000 (33 * 210000), subsidy should be 0
		supply1 := CalculateExpectedSupplyAtHeight(6930000, &chaincfg.MainNetParams)
		supply2 := CalculateExpectedSupplyAtHeight(6929999, &chaincfg.MainNetParams)

		// Both should be equal since subsidy is 0 at height 6930000
		if supply1 != supply2 {
			t.Errorf("Expected same supply at height 6930000 and 6929999, got %d vs %d", supply1, supply2)
		}
	})
}

func TestCalculateExpectedSupplyAtHeight_ManualCalculation(t *testing.T) {
	// Test manual calculation for a small height to verify the logic
	height := uint32(5)
	expected := uint64(6) * 5000000000 // 6 blocks (0 through 5) * 50 BTC each

	supply := CalculateExpectedSupplyAtHeight(height, &chaincfg.MainNetParams)
	if supply != expected {
		t.Errorf("CalculateExpectedSupplyAtHeight(%d) = %d, want %d", height, supply, expected)
	}
}

func TestCalculateExpectedSupplyAtHeight_HalvingBoundaries(t *testing.T) {
	// Test the boundaries around halving events to ensure proper calculation
	tests := []struct {
		name   string
		height uint32
	}{
		{"Just before first halving", 209999},
		{"First halving block", 210000},
		{"Just after first halving", 210001},
		{"Just before second halving", 419999},
		{"Second halving block", 420000},
		{"Just after second halving", 420001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			supply := CalculateExpectedSupplyAtHeight(tt.height, &chaincfg.MainNetParams)

			// Basic sanity checks
			if supply == 0 {
				t.Errorf("Supply should not be 0 at height %d", tt.height)
			}

			// Supply should increase with height (at least until subsidies become 0)
			if tt.height > 0 {
				prevSupply := CalculateExpectedSupplyAtHeight(tt.height-1, &chaincfg.MainNetParams)
				if supply <= prevSupply {
					t.Errorf("Supply should increase at height %d: prev=%d, curr=%d",
						tt.height, prevSupply, supply)
				}
			}
		})
	}
}

// Benchmark the supply calculation function
func BenchmarkCalculateExpectedSupplyAtHeight(b *testing.B) {
	height := uint32(746044) // Example height from the user's UTXO file

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CalculateExpectedSupplyAtHeight(height, &chaincfg.MainNetParams)
	}
}
