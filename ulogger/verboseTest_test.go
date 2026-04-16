package ulogger

import (
	"sync"
	"testing"
)

func TestNewVerboseTestLogger(t *testing.T) {
	logger := NewVerboseTestLogger(t)
	if logger == nil {
		t.Fatal("Expected logger to be created")
	}
	if logger.t != t {
		t.Error("Expected testing.T to be set correctly")
	}
}

func TestVerboseTestLogger_LogLevel(t *testing.T) {
	logger := NewVerboseTestLogger(t)
	if logger.LogLevel() != 0 {
		t.Errorf("Expected LogLevel to return 0, got %d", logger.LogLevel())
	}
}

func TestVerboseTestLogger_SetLogLevel(t *testing.T) {
	logger := NewVerboseTestLogger(t)
	// This is a no-op, just ensure it doesn't panic
	logger.SetLogLevel("debug")
	logger.SetLogLevel("info")
}

func TestVerboseTestLogger_New(t *testing.T) {
	logger := NewVerboseTestLogger(t)
	newLogger := logger.New("test-service")
	if newLogger != logger {
		t.Error("Expected New to return the same logger instance")
	}
}

func TestVerboseTestLogger_Duplicate(t *testing.T) {
	logger := NewVerboseTestLogger(t)
	dupLogger := logger.Duplicate()
	if dupLogger != logger {
		t.Error("Expected Duplicate to return the same logger instance")
	}
}

func TestVerboseTestLogger_Debugf(t *testing.T) {
	logger := NewVerboseTestLogger(t)
	// Just ensure it doesn't panic
	logger.Debugf("test message: %s", "debug")
}

func TestVerboseTestLogger_Infof(t *testing.T) {
	logger := NewVerboseTestLogger(t)
	// Just ensure it doesn't panic
	logger.Infof("test message: %s", "info")
}

func TestVerboseTestLogger_Warnf(t *testing.T) {
	logger := NewVerboseTestLogger(t)
	// Just ensure it doesn't panic
	logger.Warnf("test message: %s", "warn")
}

func TestVerboseTestLogger_Errorf(t *testing.T) {
	logger := NewVerboseTestLogger(t)
	// Just ensure it doesn't panic
	logger.Errorf("test message: %s", "error")
}

func TestVerboseTestLogger_NilT(t *testing.T) {
	logger := &VerboseTestLogger{t: nil}

	// These should not panic even with nil t
	logger.Debugf("test")
	logger.Infof("test")
	logger.Warnf("test")
	logger.Errorf("test")
	logger.Fatalf("test")
}

func TestVerboseTestLogger_Concurrency(t *testing.T) {
	logger := NewVerboseTestLogger(t)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			logger.Debugf("concurrent log %d", n)
			logger.Infof("concurrent log %d", n)
			logger.Warnf("concurrent log %d", n)
			logger.Errorf("concurrent log %d", n)
		}(i)
	}
	wg.Wait()
}
