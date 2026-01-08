package tui

import (
	"time"

	"surge/internal/downloader"
)

const (
	// Timeouts and Intervals
	TickInterval = 200 * time.Millisecond
	// Input Dimensions
	InputWidth = 40

	// Layout Offsets and Padding
	HeaderWidthOffset      = 2
	ProgressBarWidthOffset = 4
	DefaultPaddingX        = 1
	DefaultPaddingY        = 0
	PopupPaddingY          = 1
	PopupPaddingX          = 2
	PopupWidth             = 70 // Consistent width for all popup dialogs

	// Viewport layout
	CardHeight       = 5  // Each download card takes ~5 lines
	HeaderHeight     = 4  // Header + stats + spacing
	FilePickerHeight = 12 // Height for file picker display

	// Channel Buffers - use consolidated constant from downloader
	ProgressChannelBuffer = downloader.ProgressChannelBuffer

	// Units - use consolidated constant from downloader
	Megabyte = downloader.Megabyte
)
