package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/surge-downloader/surge/internal/config"
)

var (
	debugFile *os.File
	debugOnce sync.Once
)

// Debug writes a message to debug.log file in the global surge logs directory
func Debug(format string, args ...any) {
	// add timestamp to each debug message
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	debugOnce.Do(func() {
		logsDir := config.GetLogsDir()
		os.MkdirAll(logsDir, 0755)
		debugFile, _ = os.Create(filepath.Join(logsDir, fmt.Sprintf("debug-%s.log", time.Now().Format("20060102-150405"))))
	})
	if debugFile != nil {
		fmt.Fprintf(debugFile, "[%s] %s\n", timestamp, fmt.Sprintf(format, args...))
	}
}
