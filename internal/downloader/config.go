package downloader

import (
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// Size constants
const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB

	// Megabyte as float for display calculations
	Megabyte = 1024.0 * 1024.0
)

// Chunk size constants for concurrent downloads
const (
	MinChunk     = 2 * MB  // Minimum chunk size
	MaxChunk     = 16 * MB // Maximum chunk size
	TargetChunk  = 8 * MB  // Target chunk size
	AlignSize    = 4 * KB  // Align chunks to 4KB for filesystem
	WorkerBuffer = 512 * KB

	TasksPerWorker = 4 // Target tasks per connection
)

// Connection limits
const (
	PerHostMax = 8 // Max concurrent connections per host
)

// HTTP Client Tuning
const (
	DefaultMaxIdleConns          = 100
	DefaultIdleConnTimeout       = 90 * time.Second
	DefaultTLSHandshakeTimeout   = 10 * time.Second
	DefaultResponseHeaderTimeout = 15 * time.Second
	DefaultExpectContinueTimeout = 1 * time.Second
	DialTimeout                  = 10 * time.Second
	KeepAliveDuration            = 30 * time.Second
	ProbeTimeout                 = 10 * time.Second
)

// Channel buffer sizes
const (
	ProgressChannelBuffer = 16
)

// DownloadConfig contains all parameters needed to start a download
type DownloadConfig struct {
	URL        string
	OutputPath string
	ID         int
	Verbose    bool
	MD5Sum     string
	SHA256Sum  string
	ProgressCh chan<- tea.Msg
	State      *ProgressState
}
