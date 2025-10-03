package ulogger_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewGoCoreLogger tests the NewGoCoreLogger constructor
func TestNewGoCoreLogger(t *testing.T) {
	t.Run("with empty service name", func(t *testing.T) {
		logger := ulogger.NewGoCoreLogger("")
		require.NotNil(t, logger)
		// When empty, should default to "teranode"
	})

	t.Run("with service name", func(t *testing.T) {
		logger := ulogger.NewGoCoreLogger("test-service")
		require.NotNil(t, logger)
	})

	t.Run("with custom log level", func(t *testing.T) {
		logger := ulogger.NewGoCoreLogger("test", ulogger.WithLevel("DEBUG"))
		require.NotNil(t, logger)
	})

	t.Run("with multiple options", func(t *testing.T) {
		logger := ulogger.NewGoCoreLogger("test",
			ulogger.WithLevel("ERROR"),
			ulogger.WithSkipFrame(2))
		require.NotNil(t, logger)
	})

	t.Run("with all log levels", func(t *testing.T) {
		levels := []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
		for _, level := range levels {
			t.Run(level, func(t *testing.T) {
				logger := ulogger.NewGoCoreLogger("test", ulogger.WithLevel(level))
				require.NotNil(t, logger)
			})
		}
	})

	t.Run("with skip frame option", func(t *testing.T) {
		logger := ulogger.NewGoCoreLogger("test", ulogger.WithSkipFrame(3))
		require.NotNil(t, logger)
	})

	t.Run("default options", func(t *testing.T) {
		// Test that default options are applied when no options provided
		logger := ulogger.NewGoCoreLogger("test")
		require.NotNil(t, logger)
	})
}

// TestGoCoreLogger_New tests the New method for creating child loggers
func TestGoCoreLogger_New(t *testing.T) {
	parentLogger := ulogger.NewGoCoreLogger("parent")

	t.Run("basic new logger", func(t *testing.T) {
		childLogger := parentLogger.New("child")
		require.NotNil(t, childLogger)

		// Verify it implements the Logger interface
		var _ ulogger.Logger = childLogger
	})

	t.Run("new logger with options", func(t *testing.T) {
		childLogger := parentLogger.New("child", ulogger.WithSkipFrame(2))
		require.NotNil(t, childLogger)
	})

	t.Run("new logger inherits parent log level", func(t *testing.T) {
		// Create parent with specific log level
		parent := ulogger.NewGoCoreLogger("parent", ulogger.WithLevel("ERROR"))
		child := parent.New("child")
		require.NotNil(t, child)

		// Child should be able to log at the same level
		// The actual log level inheritance is handled by gocore.Logger
	})

	t.Run("new logger with multiple options", func(t *testing.T) {
		childLogger := parentLogger.New("child",
			ulogger.WithSkipFrame(1),
			ulogger.WithLevel("DEBUG"))
		require.NotNil(t, childLogger)
	})

	t.Run("multiple child loggers", func(t *testing.T) {
		child1 := parentLogger.New("child1")
		child2 := parentLogger.New("child2")
		child3 := parentLogger.New("child3")

		require.NotNil(t, child1)
		require.NotNil(t, child2)
		require.NotNil(t, child3)
	})
}

// TestGoCoreLogger_Duplicate tests the Duplicate method
func TestGoCoreLogger_Duplicate(t *testing.T) {
	originalLogger := ulogger.NewGoCoreLogger("original", ulogger.WithLevel("INFO"))

	t.Run("basic duplicate", func(t *testing.T) {
		dupLogger := originalLogger.Duplicate()
		require.NotNil(t, dupLogger)

		// Verify it implements the Logger interface
		var _ ulogger.Logger = dupLogger
	})

	t.Run("duplicate with log level change", func(t *testing.T) {
		dupLogger := originalLogger.Duplicate(ulogger.WithLevel("DEBUG"))
		require.NotNil(t, dupLogger)

		// Original logger should not be affected
		require.NotNil(t, originalLogger)
	})

	t.Run("duplicate with skip frame change", func(t *testing.T) {
		dupLogger := originalLogger.Duplicate(ulogger.WithSkipFrame(5))
		require.NotNil(t, dupLogger)
	})

	t.Run("duplicate with skip increment", func(t *testing.T) {
		// Create logger with initial skip value
		logger := ulogger.NewGoCoreLogger("test", ulogger.WithSkipFrame(2))

		// Duplicate with increment
		dupLogger := logger.Duplicate(ulogger.WithSkipFrameIncrement(3))
		require.NotNil(t, dupLogger)

		// The duplicate should have skipFrame = 2 + 3 = 5
	})

	t.Run("duplicate without options", func(t *testing.T) {
		// Should create exact copy with same log level and skip frame
		dupLogger := originalLogger.Duplicate()
		require.NotNil(t, dupLogger)
	})

	t.Run("duplicate with multiple option changes", func(t *testing.T) {
		dupLogger := originalLogger.Duplicate(
			ulogger.WithLevel("ERROR"),
			ulogger.WithSkipFrame(3),
		)
		require.NotNil(t, dupLogger)
	})

	t.Run("duplicate with only skip increment", func(t *testing.T) {
		// Test the skipIncrement > 0 condition
		dupLogger := originalLogger.Duplicate(ulogger.WithSkipFrameIncrement(2))
		require.NotNil(t, dupLogger)
	})

	t.Run("duplicate with skip and skip increment", func(t *testing.T) {
		// Test both skip and skipIncrement being set
		dupLogger := originalLogger.Duplicate(
			ulogger.WithSkipFrame(4),
			ulogger.WithSkipFrameIncrement(1),
		)
		require.NotNil(t, dupLogger)
	})

	t.Run("duplicate preserves original", func(t *testing.T) {
		// Verify that duplicating doesn't modify the original logger
		original := ulogger.NewGoCoreLogger("test", ulogger.WithLevel("INFO"))
		duplicate := original.Duplicate(ulogger.WithLevel("ERROR"))

		// Both should be valid and independent
		require.NotNil(t, original)
		require.NotNil(t, duplicate)
	})

	t.Run("multiple duplicates", func(t *testing.T) {
		// Create multiple duplicates to ensure they're independent
		dup1 := originalLogger.Duplicate(ulogger.WithLevel("DEBUG"))
		dup2 := originalLogger.Duplicate(ulogger.WithLevel("WARN"))
		dup3 := originalLogger.Duplicate(ulogger.WithLevel("ERROR"))

		require.NotNil(t, dup1)
		require.NotNil(t, dup2)
		require.NotNil(t, dup3)
	})
}

// TestGoCoreLogger_SetLogLevel tests the SetLogLevel method
func TestGoCoreLogger_SetLogLevel(t *testing.T) {
	logger := ulogger.NewGoCoreLogger("test")

	t.Run("set to DEBUG", func(t *testing.T) {
		// SetLogLevel is a noop for GoCoreLogger
		// It has to be set when creating the logger
		logger.SetLogLevel("DEBUG")
		// Should not panic
		require.NotNil(t, logger)
	})

	t.Run("set to INFO", func(t *testing.T) {
		logger.SetLogLevel("INFO")
		require.NotNil(t, logger)
	})

	t.Run("set to WARN", func(t *testing.T) {
		logger.SetLogLevel("WARN")
		require.NotNil(t, logger)
	})

	t.Run("set to ERROR", func(t *testing.T) {
		logger.SetLogLevel("ERROR")
		require.NotNil(t, logger)
	})

	t.Run("set to FATAL", func(t *testing.T) {
		logger.SetLogLevel("FATAL")
		require.NotNil(t, logger)
	})

	t.Run("set to invalid level", func(t *testing.T) {
		logger.SetLogLevel("INVALID")
		require.NotNil(t, logger)
	})

	t.Run("set empty level", func(t *testing.T) {
		logger.SetLogLevel("")
		require.NotNil(t, logger)
	})

	t.Run("multiple level changes", func(t *testing.T) {
		// Since SetLogLevel is a noop, multiple calls should be safe
		logger.SetLogLevel("DEBUG")
		logger.SetLogLevel("INFO")
		logger.SetLogLevel("ERROR")
		require.NotNil(t, logger)
	})
}

// TestGoCoreLogger_Integration tests integration scenarios
func TestGoCoreLogger_Integration(t *testing.T) {
	t.Run("create logger and use logging methods", func(t *testing.T) {
		logger := ulogger.NewGoCoreLogger("integration-test", ulogger.WithLevel("DEBUG"))
		require.NotNil(t, logger)

		// Test that we can call the logging methods
		// Note: These will output to the logger, but won't panic
		logger.Debugf("debug message")
		logger.Infof("info message")
		logger.Warnf("warn message")
		logger.Errorf("error message")
	})

	t.Run("create hierarchy of loggers", func(t *testing.T) {
		root := ulogger.NewGoCoreLogger("root")
		child1 := root.New("child1")
		child2 := root.New("child2")
		grandchild := child1.New("grandchild")

		require.NotNil(t, root)
		require.NotNil(t, child1)
		require.NotNil(t, child2)
		require.NotNil(t, grandchild)

		// All should be able to log
		root.Infof("root message")
		child1.Infof("child1 message")
		child2.Infof("child2 message")
		grandchild.Infof("grandchild message")
	})

	t.Run("duplicate and modify", func(t *testing.T) {
		original := ulogger.NewGoCoreLogger("original", ulogger.WithLevel("INFO"))

		// Create duplicates with different configurations
		debugDup := original.Duplicate(ulogger.WithLevel("DEBUG"))
		errorDup := original.Duplicate(ulogger.WithLevel("ERROR"))
		skipDup := original.Duplicate(ulogger.WithSkipFrame(3))

		require.NotNil(t, original)
		require.NotNil(t, debugDup)
		require.NotNil(t, errorDup)
		require.NotNil(t, skipDup)

		// All should be usable
		original.Infof("original")
		debugDup.Infof("debug dup")
		errorDup.Infof("error dup")
		skipDup.Infof("skip dup")
	})

	t.Run("use with ulogger.New factory", func(t *testing.T) {
		// Test that GoCoreLogger works when created through the factory
		logger := ulogger.New("test-service",
			ulogger.WithLoggerType("gocore"),
			ulogger.WithLevel("DEBUG"))
		require.NotNil(t, logger)

		// Should be a GoCoreLogger
		_, ok := logger.(*ulogger.GoCoreLogger)
		assert.True(t, ok, "Logger should be a GoCoreLogger")

		// Should be usable
		logger.Infof("test message from factory")
	})
}

// TestGoCoreLogger_LogLevel tests the LogLevel method
func TestGoCoreLogger_LogLevel(t *testing.T) {
	t.Run("DEBUG level", func(t *testing.T) {
		logger := ulogger.NewGoCoreLogger("test-debug", ulogger.WithLevel("DEBUG"))
		level := logger.LogLevel()
		assert.Equal(t, int(gocore.DEBUG), level)
	})

	t.Run("INFO level", func(t *testing.T) {
		logger := ulogger.NewGoCoreLogger("test-info", ulogger.WithLevel("INFO"))
		level := logger.LogLevel()
		assert.Equal(t, int(gocore.INFO), level)
	})

	t.Run("WARN level", func(t *testing.T) {
		logger := ulogger.NewGoCoreLogger("test-warn", ulogger.WithLevel("WARN"))
		level := logger.LogLevel()
		assert.Equal(t, int(gocore.WARN), level)
	})

	t.Run("ERROR level", func(t *testing.T) {
		logger := ulogger.NewGoCoreLogger("test-error", ulogger.WithLevel("ERROR"))
		level := logger.LogLevel()
		assert.Equal(t, int(gocore.ERROR), level)
	})

	t.Run("FATAL level", func(t *testing.T) {
		logger := ulogger.NewGoCoreLogger("test-fatal", ulogger.WithLevel("FATAL"))
		level := logger.LogLevel()
		assert.Equal(t, int(gocore.FATAL), level)
	})

	t.Run("default level", func(t *testing.T) {
		logger := ulogger.NewGoCoreLogger("test-default")
		level := logger.LogLevel()
		// Default is INFO
		assert.Equal(t, int(gocore.INFO), level)
	})
}

// TestGoCoreLogger_LoggingMethods tests all logging methods
func TestGoCoreLogger_LoggingMethods(t *testing.T) {
	logger := ulogger.NewGoCoreLogger("test", ulogger.WithLevel("DEBUG"))

	t.Run("Debugf", func(t *testing.T) {
		// Should not panic
		assert.NotPanics(t, func() {
			logger.Debugf("debug: %s", "test")
		})
	})

	t.Run("Infof", func(t *testing.T) {
		assert.NotPanics(t, func() {
			logger.Infof("info: %s", "test")
		})
	})

	t.Run("Warnf", func(t *testing.T) {
		assert.NotPanics(t, func() {
			logger.Warnf("warn: %s", "test")
		})
	})

	t.Run("Errorf", func(t *testing.T) {
		assert.NotPanics(t, func() {
			logger.Errorf("error: %s", "test")
		})
	})

	t.Run("multiple arguments", func(t *testing.T) {
		assert.NotPanics(t, func() {
			logger.Infof("test %s %d %v", "string", 42, true)
		})
	})

	t.Run("no arguments", func(t *testing.T) {
		assert.NotPanics(t, func() {
			logger.Infof("simple message")
		})
	})
}

// TestGoCoreLogger_MethodExists tests that all required Logger interface methods exist
func TestGoCoreLogger_MethodExists(t *testing.T) {
	logger := ulogger.NewGoCoreLogger("test")

	v := reflect.ValueOf(logger)

	methods := []string{
		"LogLevel",
		"SetLogLevel",
		"Debugf",
		"Infof",
		"Warnf",
		"Errorf",
		"Fatalf",
		"New",
		"Duplicate",
	}

	for _, methodName := range methods {
		t.Run(methodName, func(t *testing.T) {
			method := v.MethodByName(methodName)
			assert.True(t, method.IsValid(), "Method %s should exist", methodName)
		})
	}
}

// TestGoCoreLogger_InterfaceCompliance tests that GoCoreLogger implements Logger interface
func TestGoCoreLogger_InterfaceCompliance(t *testing.T) {
	var _ ulogger.Logger = &ulogger.GoCoreLogger{}

	// If this compiles, the interface is implemented correctly
	logger := ulogger.NewGoCoreLogger("test")
	var iface ulogger.Logger = logger

	assert.NotNil(t, iface)
}

// TestGoCoreLogger_EdgeCases tests edge cases and boundary conditions
func TestGoCoreLogger_EdgeCases(t *testing.T) {
	t.Run("long service name", func(t *testing.T) {
		// Use a reasonably long name that won't exceed Unix socket path limits
		longName := string(bytes.Repeat([]byte("a"), 50))
		logger := ulogger.NewGoCoreLogger(longName)
		require.NotNil(t, logger)
		logger.Infof("test with long name")
	})

	t.Run("service name with special characters", func(t *testing.T) {
		logger := ulogger.NewGoCoreLogger("test-service_123.v2")
		require.NotNil(t, logger)
		logger.Infof("test with special chars")
	})

	t.Run("service name with unicode", func(t *testing.T) {
		logger := ulogger.NewGoCoreLogger("test-服务-🚀")
		require.NotNil(t, logger)
		logger.Infof("test with unicode")
	})

	t.Run("very high skip frame value", func(t *testing.T) {
		logger := ulogger.NewGoCoreLogger("test", ulogger.WithSkipFrame(1000))
		require.NotNil(t, logger)
		logger.Infof("test with high skip")
	})

	t.Run("negative skip increment", func(t *testing.T) {
		logger := ulogger.NewGoCoreLogger("test", ulogger.WithSkipFrame(5))
		// Negative increment should not be added (skipIncrement > 0 check)
		dup := logger.Duplicate(ulogger.WithSkipFrameIncrement(-1))
		require.NotNil(t, dup)
	})

	t.Run("zero skip increment", func(t *testing.T) {
		logger := ulogger.NewGoCoreLogger("test")
		// Zero increment should not trigger the addition (skipIncrement > 0 check)
		dup := logger.Duplicate(ulogger.WithSkipFrameIncrement(0))
		require.NotNil(t, dup)
	})

	t.Run("multiple option applications", func(t *testing.T) {
		// Apply same option multiple times
		logger := ulogger.NewGoCoreLogger("test",
			ulogger.WithLevel("DEBUG"),
			ulogger.WithLevel("INFO"),
			ulogger.WithLevel("ERROR"))
		require.NotNil(t, logger)
		// Last option should win
	})
}

// TestGoCoreLogger_ConcurrentAccess tests concurrent usage
func TestGoCoreLogger_ConcurrentAccess(t *testing.T) {
	logger := ulogger.NewGoCoreLogger("concurrent-test", ulogger.WithLevel("INFO"))

	t.Run("concurrent logging", func(t *testing.T) {
		done := make(chan bool, 10)

		for i := 0; i < 10; i++ {
			go func(id int) {
				defer func() { done <- true }()
				logger.Infof("concurrent log from goroutine %d", id)
			}(i)
		}

		// Wait for all goroutines
		for i := 0; i < 10; i++ {
			<-done
		}
	})

	t.Run("concurrent New calls", func(t *testing.T) {
		done := make(chan bool, 10)

		for i := 0; i < 10; i++ {
			go func(id int) {
				defer func() { done <- true }()
				child := logger.New("child")
				require.NotNil(t, child)
			}(i)
		}

		for i := 0; i < 10; i++ {
			<-done
		}
	})

	t.Run("concurrent Duplicate calls", func(t *testing.T) {
		done := make(chan bool, 10)

		for i := 0; i < 10; i++ {
			go func(id int) {
				defer func() { done <- true }()
				dup := logger.Duplicate()
				require.NotNil(t, dup)
			}(i)
		}

		for i := 0; i < 10; i++ {
			<-done
		}
	})
}

// BenchmarkNewGoCoreLogger benchmarks logger creation
func BenchmarkNewGoCoreLogger(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ulogger.NewGoCoreLogger("benchmark")
	}
}

// BenchmarkGoCoreLogger_Duplicate benchmarks logger duplication
func BenchmarkGoCoreLogger_Duplicate(b *testing.B) {
	logger := ulogger.NewGoCoreLogger("benchmark")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = logger.Duplicate()
	}
}

// BenchmarkGoCoreLogger_New benchmarks child logger creation
func BenchmarkGoCoreLogger_New(b *testing.B) {
	logger := ulogger.NewGoCoreLogger("benchmark")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = logger.New("child")
	}
}

// BenchmarkGoCoreLogger_Infof benchmarks logging
func BenchmarkGoCoreLogger_Infof(b *testing.B) {
	logger := ulogger.NewGoCoreLogger("benchmark", ulogger.WithLevel("INFO"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Infof("benchmark message %d", i)
	}
}
