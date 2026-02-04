package txmetacache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBucketType_String(t *testing.T) {
	tests := []struct {
		name     string
		bt       BucketType
		expected string
	}{
		{"Unallocated", Unallocated, "Unallocated"},
		{"Preallocated", Preallocated, "Preallocated"},
		{"Trimmed", Trimmed, "Trimmed"},
		{"Clock", Clock, "Clock"},
		{"Unknown", BucketType(999), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.bt.String()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestParseBucketType(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected BucketType
	}{
		// Exact case matches
		{"Unallocated exact", "Unallocated", Unallocated},
		{"Preallocated exact", "Preallocated", Preallocated},
		{"Trimmed exact", "Trimmed", Trimmed},
		{"Clock exact", "Clock", Clock},

		// Lowercase variants
		{"unallocated lowercase", "unallocated", Unallocated},
		{"preallocated lowercase", "preallocated", Preallocated},
		{"trimmed lowercase", "trimmed", Trimmed},
		{"clock lowercase", "clock", Clock},

		// Invalid/unknown defaults to Clock
		{"empty string", "", Clock},
		{"invalid string", "InvalidType", Clock},
		{"random string", "xyz", Clock},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseBucketType(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestParseBucketType_RoundTrip(t *testing.T) {
	// Test that parsing the string representation returns the same bucket type
	bucketTypes := []BucketType{Unallocated, Preallocated, Trimmed, Clock}

	for _, bt := range bucketTypes {
		t.Run(bt.String(), func(t *testing.T) {
			str := bt.String()
			parsed := ParseBucketType(str)
			require.Equal(t, bt, parsed, "Round trip failed for %s", str)
		})
	}
}
