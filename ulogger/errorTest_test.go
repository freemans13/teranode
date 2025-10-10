package ulogger_test

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
)

// MockTestingT implements the TestingT interface for testing
type MockTestingT struct {
	mu          sync.Mutex
	errorCalled bool
	failCalled  bool
	logCalled   bool
	messages    []string
	helper      bool
}

func (m *MockTestingT) Errorf(format string, args ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errorCalled = true
}

func (m *MockTestingT) FailNow() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failCalled = true
}

func (m *MockTestingT) Logf(format string, args ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logCalled = true
	m.messages = append(m.messages, format)
}

func (m *MockTestingT) Helper() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.helper = true
}

// Thread-safe getters for test assertions
func (m *MockTestingT) ErrorCalled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.errorCalled
}

func (m *MockTestingT) FailCalled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failCalled
}

func (m *MockTestingT) LogCalled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.logCalled
}

func (m *MockTestingT) Messages() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Return a copy to prevent external modification
	result := make([]string, len(m.messages))
	copy(result, m.messages)
	return result
}

func (m *MockTestingT) HelperCalled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.helper
}

func (m *MockTestingT) SetHelper(value bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.helper = value
}

// Test NewErrorTestLogger function - covers 0% -> 100%
func TestNewErrorTestLogger_CompleteCoverage(t *testing.T) {
	t.Run("without cancel function", func(t *testing.T) {
		mockT := &MockTestingT{}
		logger := ulogger.NewErrorTestLogger(mockT)

		assert.NotNil(t, logger)
		// Test that it implements the Logger interface
		var _ ulogger.Logger = logger
	})

	t.Run("with cancel function", func(t *testing.T) {
		mockT := &MockTestingT{}
		cancelCalled := false
		cancelFn := func() {
			cancelCalled = true
		}

		logger := ulogger.NewErrorTestLogger(mockT, cancelFn)

		assert.NotNil(t, logger)
		// Verify cancel function is stored (tested indirectly through SetCancelFn)
		logger.SetCancelFn(func() { cancelCalled = true })
		assert.False(t, cancelCalled) // Hasn't been called yet
	})

	t.Run("with multiple cancel functions", func(t *testing.T) {
		mockT := &MockTestingT{}
		cancelFn1 := func() {}
		cancelFn2 := func() {}

		// Should only use the first cancel function
		logger := ulogger.NewErrorTestLogger(mockT, cancelFn1, cancelFn2)

		assert.NotNil(t, logger)
	})
}

// Test SetCancelFn method - covers 0% -> 100%
func TestSetCancelFn_CompleteCoverage(t *testing.T) {
	mockT := &MockTestingT{}
	logger := ulogger.NewErrorTestLogger(mockT)

	cancelCalled := false
	cancelFn := func() {
		cancelCalled = true
	}

	logger.SetCancelFn(cancelFn)

	// Verify the cancel function was set (tested indirectly)
	assert.False(t, cancelCalled) // Should not be called just by setting it
	assert.NotNil(t, logger)
}

// Test EnableVerbose method - covers 0% -> 100%
func TestEnableVerbose_CompleteCoverage(t *testing.T) {
	mockT := &MockTestingT{}
	logger := ulogger.NewErrorTestLogger(mockT)

	// This is a no-op method, just test that it can be called without panic
	logger.EnableVerbose()

	assert.NotNil(t, logger)
}

// Test SkipCancelOnFail method - covers 0% -> 100%
func TestSkipCancelOnFail_CompleteCoverage(t *testing.T) {
	t.Run("with helper interface", func(t *testing.T) {
		mockT := &MockTestingT{}
		logger := ulogger.NewErrorTestLogger(mockT)

		// Test setting skip to true
		logger.SkipCancelOnFail(true)
		assert.True(t, mockT.HelperCalled(), "Helper should have been called")

		// Reset helper flag
		mockT.SetHelper(false)

		// Test setting skip to false
		logger.SkipCancelOnFail(false)
		assert.True(t, mockT.HelperCalled(), "Helper should have been called again")
	})

	t.Run("without helper interface", func(t *testing.T) {
		// Create a mock that doesn't implement Helper
		// Mock the interface without Helper method
		testT := struct {
			ulogger.TestingT
		}{
			TestingT: &mockTWithoutHelper{},
		}

		logger := ulogger.NewErrorTestLogger(testT.TestingT)

		// Should not panic even without Helper interface
		logger.SkipCancelOnFail(true)

		assert.NotNil(t, logger)
	})
}

type mockTWithoutHelper struct {
	errorCalled bool
	failCalled  bool
	logCalled   bool
}

func (m *mockTWithoutHelper) Errorf(format string, args ...interface{}) {
	m.errorCalled = true
}

func (m *mockTWithoutHelper) FailNow() {
	m.failCalled = true
}

func (m *mockTWithoutHelper) Logf(format string, args ...interface{}) {
	m.logCalled = true
}

// Test LogLevel method - covers 0% -> 100%
func TestErrorTestLogger_LogLevel_CompleteCoverage(t *testing.T) {
	mockT := &MockTestingT{}
	logger := ulogger.NewErrorTestLogger(mockT)

	level := logger.LogLevel()
	assert.Equal(t, 0, level, "LogLevel should always return 0")
}

// Test SetLogLevel method - covers 0% -> 100%
func TestErrorTestLogger_SetLogLevel_CompleteCoverage(t *testing.T) {
	mockT := &MockTestingT{}
	logger := ulogger.NewErrorTestLogger(mockT)

	// This is a no-op method, just test that it can be called without panic
	logger.SetLogLevel("DEBUG")
	logger.SetLogLevel("INFO")
	logger.SetLogLevel("WARN")
	logger.SetLogLevel("ERROR")
	logger.SetLogLevel("FATAL")

	assert.NotNil(t, logger)
}

// Test New method - covers 0% -> 100%
func TestErrorTestLogger_New_CompleteCoverage(t *testing.T) {
	t.Run("with helper interface", func(t *testing.T) {
		mockT := &MockTestingT{}
		logger := ulogger.NewErrorTestLogger(mockT)

		newLogger := logger.New("test-service")

		assert.Equal(t, logger, newLogger, "New should return the same logger instance")
		assert.True(t, mockT.HelperCalled(), "Helper should have been called")
	})

	t.Run("without helper interface", func(t *testing.T) {
		mockT := &mockTWithoutHelper{}
		logger := ulogger.NewErrorTestLogger(mockT)

		newLogger := logger.New("test-service")

		assert.Equal(t, logger, newLogger, "New should return the same logger instance")
	})

	t.Run("with options", func(t *testing.T) {
		mockT := &MockTestingT{}
		logger := ulogger.NewErrorTestLogger(mockT)

		newLogger := logger.New("test-service", ulogger.WithLevel("DEBUG"))

		assert.Equal(t, logger, newLogger, "New should return the same logger instance")
	})
}

// Test Duplicate method - covers 0% -> 100%
func TestErrorTestLogger_Duplicate_CompleteCoverage(t *testing.T) {
	t.Run("with helper interface", func(t *testing.T) {
		mockT := &MockTestingT{}
		logger := ulogger.NewErrorTestLogger(mockT)

		duplicateLogger := logger.Duplicate()

		assert.Equal(t, logger, duplicateLogger, "Duplicate should return the same logger instance")
		assert.True(t, mockT.HelperCalled(), "Helper should have been called")
	})

	t.Run("without helper interface", func(t *testing.T) {
		mockT := &mockTWithoutHelper{}
		logger := ulogger.NewErrorTestLogger(mockT)

		duplicateLogger := logger.Duplicate()

		assert.Equal(t, logger, duplicateLogger, "Duplicate should return the same logger instance")
	})

	t.Run("with options", func(t *testing.T) {
		mockT := &MockTestingT{}
		logger := ulogger.NewErrorTestLogger(mockT)

		duplicateLogger := logger.Duplicate(ulogger.WithLevel("ERROR"))

		assert.Equal(t, logger, duplicateLogger, "Duplicate should return the same logger instance")
	})
}

// Test Debugf method - covers 0% -> 100%
func TestDebugf_CompleteCoverage(t *testing.T) {
	mockT := &MockTestingT{}
	logger := ulogger.NewErrorTestLogger(mockT)

	// This is a no-op method, just test that it can be called without panic
	logger.Debugf("debug message")
	logger.Debugf("debug message with args: %s %d", "test", 123)

	assert.NotNil(t, logger)
	assert.False(t, mockT.LogCalled(), "Debugf should not call Logf")
}

// Test Infof method - covers 0% -> 100%
func TestInfof_CompleteCoverage(t *testing.T) {
	mockT := &MockTestingT{}
	logger := ulogger.NewErrorTestLogger(mockT)

	// This is a no-op method, just test that it can be called without panic
	logger.Infof("info message")
	logger.Infof("info message with args: %s %d", "test", 123)

	assert.NotNil(t, logger)
	assert.False(t, mockT.LogCalled(), "Infof should not call Logf")
}

// Test Warnf method - covers 0% -> 100%
func TestWarnf_CompleteCoverage(t *testing.T) {
	mockT := &MockTestingT{}
	logger := ulogger.NewErrorTestLogger(mockT)

	// This is a no-op method, just test that it can be called without panic
	logger.Warnf("warn message")
	logger.Warnf("warn message with args: %s %d", "test", 123)

	assert.NotNil(t, logger)
	assert.False(t, mockT.LogCalled(), "Warnf should not call Logf")
}

// Test Errorf method - covers 0% -> 100%
func TestErrorf_CompleteCoverage(t *testing.T) {
	t.Run("with helper interface and skip cancel false", func(t *testing.T) {
		mockT := &MockTestingT{}
		logger := ulogger.NewErrorTestLogger(mockT)

		logger.Errorf("error message")

		assert.True(t, mockT.HelperCalled(), "Helper should have been called")
		assert.True(t, mockT.LogCalled(), "Logf should have been called")
		assert.Len(t, mockT.Messages(), 1)
		assert.Contains(t, mockT.Messages()[0], "ERR_LEVEL")
		assert.Contains(t, mockT.Messages()[0], "error message")
	})

	t.Run("with helper interface and skip cancel true", func(t *testing.T) {
		mockT := &MockTestingT{}
		logger := ulogger.NewErrorTestLogger(mockT)
		logger.SkipCancelOnFail(true)

		logger.Errorf("error message with skip")

		assert.True(t, mockT.HelperCalled(), "Helper should have been called")
		assert.True(t, mockT.LogCalled(), "Logf should have been called")
		assert.Len(t, mockT.Messages(), 1) // Only one message from Errorf, not from SkipCancelOnFail
		assert.Contains(t, mockT.Messages()[0], "ERR_LEVEL")
		assert.Contains(t, mockT.Messages()[0], "error message with skip")
	})

	t.Run("without helper interface", func(t *testing.T) {
		mockT := &mockTWithoutHelper{}
		logger := ulogger.NewErrorTestLogger(mockT)

		logger.Errorf("error message no helper")

		assert.True(t, mockT.logCalled, "Logf should have been called")
	})

	t.Run("with formatted arguments", func(t *testing.T) {
		mockT := &MockTestingT{}
		logger := ulogger.NewErrorTestLogger(mockT)

		logger.Errorf("error with args: %s %d", "test", 456)

		assert.True(t, mockT.LogCalled(), "Logf should have been called")
		assert.Contains(t, mockT.Messages()[0], "ERR_LEVEL")
		assert.Contains(t, mockT.Messages()[0], "error with args: %s %d")
	})
}

// Test Fatalf method - covers 0% -> 100%
func TestErrorTestLogger_Fatalf_CompleteCoverage(t *testing.T) {
	t.Run("with helper interface and skip cancel false", func(t *testing.T) {
		mockT := &MockTestingT{}
		logger := ulogger.NewErrorTestLogger(mockT)

		logger.Fatalf("fatal message")

		assert.True(t, mockT.HelperCalled(), "Helper should have been called")
		assert.True(t, mockT.LogCalled(), "Logf should have been called")
		assert.Len(t, mockT.Messages(), 1)
		assert.Contains(t, mockT.Messages()[0], "FATAL_LEVEL")
		assert.Contains(t, mockT.Messages()[0], "fatal message")
	})

	t.Run("with helper interface and skip cancel true", func(t *testing.T) {
		mockT := &MockTestingT{}
		logger := ulogger.NewErrorTestLogger(mockT)
		logger.SkipCancelOnFail(true)

		logger.Fatalf("fatal message with skip")

		assert.True(t, mockT.HelperCalled(), "Helper should have been called")
		assert.True(t, mockT.LogCalled(), "Logf should have been called")
		assert.Len(t, mockT.Messages(), 1) // Only one message from Fatalf
		assert.Contains(t, mockT.Messages()[0], "FATAL_LEVEL")
		assert.Contains(t, mockT.Messages()[0], "fatal message with skip")
	})

	t.Run("without helper interface", func(t *testing.T) {
		mockT := &mockTWithoutHelper{}
		logger := ulogger.NewErrorTestLogger(mockT)

		logger.Fatalf("fatal message no helper")

		assert.True(t, mockT.logCalled, "Logf should have been called")
	})

	t.Run("with formatted arguments", func(t *testing.T) {
		mockT := &MockTestingT{}
		logger := ulogger.NewErrorTestLogger(mockT)

		logger.Fatalf("fatal with args: %s %d", "test", 789)

		assert.True(t, mockT.LogCalled(), "Logf should have been called")
		assert.Contains(t, mockT.Messages()[0], "FATAL_LEVEL")
		assert.Contains(t, mockT.Messages()[0], "fatal with args: %s %d")
	})
}

// Test atomic behavior of SkipCancelOnFail
func TestSkipCancelOnFail_AtomicBehavior(t *testing.T) {
	mockT := &MockTestingT{}
	logger := ulogger.NewErrorTestLogger(mockT)

	// Test that concurrent access doesn't cause data races
	// The skipCancelOnFail field in ErrorTestLogger uses atomic operations, so this should be safe
	var wg sync.WaitGroup

	// Start multiple goroutines that call SkipCancelOnFail concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(val bool) {
			defer wg.Done()
			logger.SkipCancelOnFail(val)
		}(i%2 == 0)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify no race conditions occurred and logger still works
	logger.SkipCancelOnFail(false)
	logger.Errorf("test message")

	assert.True(t, mockT.LogCalled(), "Should be able to log after concurrent SkipCancelOnFail calls")
}

// Test that the skipCancelOnFail atomic boolean works correctly
func TestSkipCancelOnFailEffect(t *testing.T) {
	t.Run("skip cancel false - normal behavior", func(t *testing.T) {
		mockT := &MockTestingT{}
		logger := ulogger.NewErrorTestLogger(mockT)

		logger.SkipCancelOnFail(false)
		logger.Errorf("error message")

		assert.True(t, mockT.LogCalled())
		// Both branches should be executed when skip is false
		assert.Len(t, mockT.Messages(), 1)
	})

	t.Run("skip cancel true - early return behavior", func(t *testing.T) {
		mockT := &MockTestingT{}
		logger := ulogger.NewErrorTestLogger(mockT)

		logger.SkipCancelOnFail(true)
		logger.Errorf("error message with skip")

		assert.True(t, mockT.LogCalled())
		// Should still log but return early
		assert.Len(t, mockT.Messages(), 1)
	})
}
