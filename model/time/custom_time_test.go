package time

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCustomTime_Scan_TimeType(t *testing.T) {
	ct := &CustomTime{}
	now := time.Now()

	err := ct.Scan(now)
	require.NoError(t, err)
	assert.Equal(t, now, ct.Time)
}

func TestCustomTime_Scan_ByteSlice_Valid(t *testing.T) {
	ct := &CustomTime{}
	timeStr := "2024-02-10 15:04:05"
	expected, _ := time.Parse(SQLiteTimestampFormat, timeStr)

	err := ct.Scan([]byte(timeStr))
	require.NoError(t, err)
	assert.Equal(t, expected, ct.Time)
}

func TestCustomTime_Scan_ByteSlice_Invalid(t *testing.T) {
	ct := &CustomTime{}
	invalidTimeStr := "invalid-time-format"

	err := ct.Scan([]byte(invalidTimeStr))
	assert.Error(t, err)
}

func TestCustomTime_Scan_String_Valid(t *testing.T) {
	ct := &CustomTime{}
	timeStr := "2024-02-10 15:04:05"
	expected, _ := time.Parse(SQLiteTimestampFormat, timeStr)

	err := ct.Scan(timeStr)
	require.NoError(t, err)
	assert.Equal(t, expected, ct.Time)
}

func TestCustomTime_Scan_String_Invalid(t *testing.T) {
	ct := &CustomTime{}
	invalidTimeStr := "not-a-valid-timestamp"

	err := ct.Scan(invalidTimeStr)
	assert.Error(t, err)
}

func TestCustomTime_Scan_UnsupportedType(t *testing.T) {
	ct := &CustomTime{}

	tests := []struct {
		name  string
		value interface{}
	}{
		{
			name:  "int type",
			value: 12345,
		},
		{
			name:  "float type",
			value: 123.45,
		},
		{
			name:  "nil value",
			value: nil,
		},
		{
			name:  "struct type",
			value: struct{}{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ct.Scan(tt.value)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "unsupported type")
		})
	}
}

func TestCustomTime_Value(t *testing.T) {
	now := time.Now()
	ct := CustomTime{Time: now}

	value, err := ct.Value()
	require.NoError(t, err)
	assert.Equal(t, now, value)
}

func TestCustomTime_Value_ZeroTime(t *testing.T) {
	ct := CustomTime{}

	value, err := ct.Value()
	require.NoError(t, err)
	assert.Equal(t, time.Time{}, value)
}

func TestCustomTime_RoundTrip_TimeType(t *testing.T) {
	ct := &CustomTime{}
	now := time.Now()

	// Scan a time.Time
	err := ct.Scan(now)
	require.NoError(t, err)

	// Get it back via Value
	value, err := ct.Value()
	require.NoError(t, err)
	assert.Equal(t, now, value)
}

func TestCustomTime_RoundTrip_String(t *testing.T) {
	ct := &CustomTime{}
	timeStr := "2024-02-10 15:04:05"

	// Scan a string
	err := ct.Scan(timeStr)
	require.NoError(t, err)

	// Get it back via Value
	value, err := ct.Value()
	require.NoError(t, err)

	// Verify it matches expected time
	expected, _ := time.Parse(SQLiteTimestampFormat, timeStr)
	assert.Equal(t, expected, value)
}

func TestCustomTime_RoundTrip_ByteSlice(t *testing.T) {
	ct := &CustomTime{}
	timeStr := "2024-02-10 15:04:05"

	// Scan a byte slice
	err := ct.Scan([]byte(timeStr))
	require.NoError(t, err)

	// Get it back via Value
	value, err := ct.Value()
	require.NoError(t, err)

	// Verify it matches expected time
	expected, _ := time.Parse(SQLiteTimestampFormat, timeStr)
	assert.Equal(t, expected, value)
}

func TestSQLiteTimestampFormat(t *testing.T) {
	// Verify the constant is correct
	assert.Equal(t, "2006-01-02 15:04:05", SQLiteTimestampFormat)

	// Verify it can parse a valid timestamp
	timeStr := "2024-02-10 15:04:05"
	parsed, err := time.Parse(SQLiteTimestampFormat, timeStr)
	require.NoError(t, err)
	assert.Equal(t, 2024, parsed.Year())
	assert.Equal(t, time.February, parsed.Month())
	assert.Equal(t, 10, parsed.Day())
	assert.Equal(t, 15, parsed.Hour())
	assert.Equal(t, 4, parsed.Minute())
	assert.Equal(t, 5, parsed.Second())
}

func TestCustomTime_Scan_EdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		input     interface{}
		shouldErr bool
	}{
		{
			name:      "empty string",
			input:     "",
			shouldErr: true,
		},
		{
			name:      "empty byte slice",
			input:     []byte(""),
			shouldErr: true,
		},
		{
			name:      "whitespace string",
			input:     "   ",
			shouldErr: true,
		},
		{
			name:      "partial timestamp",
			input:     "2024-02-10",
			shouldErr: true,
		},
		{
			name:      "timestamp with timezone",
			input:     "2024-02-10 15:04:05 UTC",
			shouldErr: true,
		},
		{
			name:      "valid timestamp at midnight",
			input:     "2024-02-10 00:00:00",
			shouldErr: false,
		},
		{
			name:      "valid timestamp at end of day",
			input:     "2024-02-10 23:59:59",
			shouldErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct := &CustomTime{}
			err := ct.Scan(tt.input)
			if tt.shouldErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCustomTime_ImplementsInterfaces(t *testing.T) {
	// Verify CustomTime implements sql.Scanner
	var _ interface {
		Scan(interface{}) error
	} = &CustomTime{}

	// Verify CustomTime implements driver.Valuer
	var _ driver.Valuer = CustomTime{}
}
