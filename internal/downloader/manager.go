package downloader

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"surge/internal/messages"
	"surge/internal/utils"

	tea "github.com/charmbracelet/bubbletea"
)

var probeClient = &http.Client{Timeout: ProbeTimeout}

var ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
	"AppleWebKit/537.36 (KHTML, like Gecko) " +
	"Chrome/120.0.0.0 Safari/537.36"

// ProbeResult contains all metadata from server probe
type ProbeResult struct {
	FileSize      int64
	SupportsRange bool
	Filename      string
	ContentType   string
}

// probeServer sends GET with Range: bytes=0-0 to determine server capabilities
func probeServer(ctx context.Context, rawurl string) (*ProbeResult, error) {
	utils.Debug("Probing server: %s", rawurl)

	probeCtx, cancel := context.WithTimeout(ctx, ProbeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, rawurl, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create probe request: %w", err)
	}

	req.Header.Set("Range", "bytes=0-0")
	req.Header.Set("User-Agent", ua)

	resp, err := probeClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("probe request failed: %w", err)
	}
	defer func() {
		io.Copy(io.Discard, resp.Body) // Drain any remaining data
		resp.Body.Close()
	}()

	utils.Debug("Probe response status: %d", resp.StatusCode)

	result := &ProbeResult{}

	// Determine range support and file size based on status code
	switch resp.StatusCode {
	case http.StatusPartialContent: // 206
		result.SupportsRange = true
		// Parse Content-Range: bytes 0-0/TOTAL
		contentRange := resp.Header.Get("Content-Range")
		utils.Debug("Content-Range header: %s", contentRange)
		if contentRange != "" {
			// Format: "bytes 0-0/12345" or "bytes 0-0/*"
			if idx := strings.LastIndex(contentRange, "/"); idx != -1 {
				sizeStr := contentRange[idx+1:]
				if sizeStr != "*" {
					result.FileSize, _ = strconv.ParseInt(sizeStr, 10, 64)
				}
			}
		}
		utils.Debug("Range supported, file size: %d", result.FileSize)

	case http.StatusOK: // 200 - server ignores Range header
		result.SupportsRange = false
		contentLength := resp.Header.Get("Content-Length")
		if contentLength != "" {
			result.FileSize, _ = strconv.ParseInt(contentLength, 10, 64)
		}
		utils.Debug("Range NOT supported (got 200), file size: %d", result.FileSize)

	default:
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Determine filename from URL and headers
	result.Filename = extractFilename(rawurl, resp)
	result.ContentType = resp.Header.Get("Content-Type")

	utils.Debug("Probe complete - filename: %s, size: %d, range: %v",
		result.Filename, result.FileSize, result.SupportsRange)

	return result, nil
}

// extractFilename gets filename from Content-Disposition or URL
func extractFilename(rawurl string, resp *http.Response) string {
	// Try Content-Disposition header first
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		if idx := strings.Index(cd, "filename="); idx != -1 {
			name := cd[idx+9:]
			name = strings.Trim(name, `"'`)
			if name != "" {
				return filepath.Base(name)
			}
		}
	}

	// Fall back to URL path
	if parsed, err := url.Parse(rawurl); err == nil {
		name := filepath.Base(parsed.Path)
		if name != "" && name != "." && name != "/" {
			return name
		}
	}

	return "download.bin"
}

// TUIDownload is the main entry point for TUI downloads
func TUIDownload(ctx context.Context, cfg DownloadConfig) error {
	// Probe server once to get all metadata
	probe, err := probeServer(ctx, cfg.URL)
	if err != nil {
		utils.Debug("Probe failed: %v", err)
		return err
	}

	// Construct proper output path
	destPath := cfg.OutputPath
	if info, err := os.Stat(cfg.OutputPath); err == nil && info.IsDir() {
		destPath = filepath.Join(cfg.OutputPath, probe.Filename)
	}
	utils.Debug("Destination path: %s", destPath)

	// Send download started message
	if cfg.ProgressCh != nil {
		cfg.ProgressCh <- messages.DownloadStartedMsg{
			DownloadID: cfg.ID,
			URL:        cfg.URL,
			Filename:   probe.Filename,
			Total:      probe.FileSize,
		}
	}

	// Update shared state
	if cfg.State != nil {
		cfg.State.SetTotalSize(probe.FileSize)
	}

	// Choose downloader based on probe results
	if probe.SupportsRange && probe.FileSize > 0 {
		utils.Debug("Using concurrent downloader")
		d := NewConcurrentDownloader(cfg.ID, cfg.ProgressCh, cfg.State)
		return d.Download(ctx, cfg.URL, destPath, probe.FileSize, cfg.Verbose)
	}

	// Fallback to single-threaded downloader
	utils.Debug("Using single-threaded downloader")
	d := NewSingleDownloader(cfg.ID, cfg.ProgressCh, cfg.State)
	return d.Download(ctx, cfg.URL, destPath, probe.FileSize, probe.Filename, cfg.Verbose)
}

// Download is the CLI entry point (non-TUI) - convenience wrapper
func Download(ctx context.Context, url, outPath string, verbose bool, md5sum, sha256sum string, progressCh chan<- tea.Msg, id int) error {
	cfg := DownloadConfig{
		URL:        url,
		OutputPath: outPath,
		ID:         id,
		Verbose:    verbose,
		MD5Sum:     md5sum,
		SHA256Sum:  sha256sum,
		ProgressCh: progressCh,
		State:      nil,
	}
	return TUIDownload(ctx, cfg)
}
