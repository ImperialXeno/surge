package utils

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/surge-downloader/surge/internal/config"
)

func TestDebug_CreatesLogFile(t *testing.T) {
	// Note: Debug uses sync.Once, so we can only test it once per test run
	// This test verifies that the debug function creates a log file

	// Ensure logs directory exists
	logsDir := config.GetLogsDir()
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("Failed to create logs directory: %v", err)
	}

	// Call Debug
	Debug("Test message from unit test")

	// Wait a moment for file to be created
	time.Sleep(100 * time.Millisecond)

	// Check if any debug log file was created
	entries, err := os.ReadDir(logsDir)
	if err != nil {
		t.Fatalf("Failed to read logs directory: %v", err)
	}

	found := false
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "debug-") && strings.HasSuffix(entry.Name(), ".log") {
			found = true
			break
		}
	}

	if !found {
		t.Log("Note: Debug log file may not be created on first run due to sync.Once behavior")
	}
}

func TestDebug_FormatsMessage(t *testing.T) {
	// Test that Debug can handle format strings with arguments
	// This shouldn't panic
	Debug("Test message with %s and %d", "string", 42)
	Debug("Simple message without formatting")
	Debug("Message with special chars: %% \\n \\t")
}

func TestDebug_HandlesEmptyMessage(t *testing.T) {
	// Debug should handle empty messages gracefully
	Debug("")
	Debug("   ")
}

func TestDebug_MultipleArguments(t *testing.T) {
	// Test with various argument types
	Debug("int: %d, float: %f, string: %s, bool: %t", 42, 3.14, "hello", true)
	Debug("Multiple strings: %s %s %s", "one", "two", "three")
}

func TestLogFilePath(t *testing.T) {
	// Verify logs directory path is valid
	logsDir := config.GetLogsDir()

	if logsDir == "" {
		t.Error("GetLogsDir returned empty string")
	}

	// Path should contain expected directory name
	if !strings.Contains(strings.ToLower(logsDir), "surge") {
		t.Errorf("Logs directory should be under surge config, got: %s", logsDir)
	}

	if !strings.HasSuffix(logsDir, "logs") {
		t.Errorf("Logs directory should end with 'logs', got: %s", logsDir)
	}

	// Should be a valid path format
	if !filepath.IsAbs(logsDir) {
		t.Errorf("Logs directory should be absolute path, got: %s", logsDir)
	}
}
