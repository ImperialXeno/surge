package downloader

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"surge/internal/utils"

	tea "github.com/charmbracelet/bubbletea"
)

// Buffer pool to reduce GC pressure
var bufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, WorkerBuffer)
		return &buf
	},
}

const (
	maxTaskRetries = 3
	retryBaseDelay = 200 * time.Millisecond
)

// ConcurrentDownloader handles multi-connection downloads
type ConcurrentDownloader struct {
	ProgressChan chan<- tea.Msg // Channel for events (start/complete/error)
	ID           int            // Download ID
	State        *ProgressState // Shared state for TUI polling
	activeTasks  map[int]*ActiveTask
	activeMu     sync.Mutex
	URL          string // For pause/resume
	DestPath     string // For pause/resume
}

// NewConcurrentDownloader creates a new concurrent downloader with all required parameters
func NewConcurrentDownloader(id int, progressCh chan<- tea.Msg, state *ProgressState) *ConcurrentDownloader {
	return &ConcurrentDownloader{
		ID:           id,
		ProgressChan: progressCh,
		State:        state,
		activeTasks:  make(map[int]*ActiveTask),
	}
}

// Task represents a byte range to download
type Task struct {
	Offset int64 // in bytes
	Length int64
}

// ActiveTask tracks a task currently being processed by a worker
type ActiveTask struct {
	Task          Task
	CurrentOffset int64 // Atomic
	StopAt        int64 // Atomic
}

// TaskQueue is a thread-safe work-stealing queue
type TaskQueue struct {
	tasks       []Task
	head        int
	mu          sync.Mutex
	cond        *sync.Cond
	done        bool
	idleWorkers int64 // Atomic counter for idle workers
}

func NewTaskQueue() *TaskQueue {
	tq := &TaskQueue{}
	tq.cond = sync.NewCond(&tq.mu)
	return tq
}

func (q *TaskQueue) Push(t Task) {
	q.mu.Lock()
	q.tasks = append(q.tasks, t)
	q.cond.Signal()
	q.mu.Unlock()
}

func (q *TaskQueue) PushMultiple(tasks []Task) {
	q.mu.Lock()
	q.tasks = append(q.tasks, tasks...)
	q.cond.Broadcast()
	q.mu.Unlock()
}

func (q *TaskQueue) Pop() (Task, bool) {
	// Mark as idle while waiting
	atomic.AddInt64(&q.idleWorkers, 1)

	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.tasks) == 0 && !q.done {
		q.cond.Wait()
	}

	// No longer idle once we have work (or are done)
	atomic.AddInt64(&q.idleWorkers, -1)

	if len(q.tasks) == 0 {
		return Task{}, false
	}

	t := q.tasks[q.head]
	q.head++
	if q.head > len(q.tasks)/2 {
		q.tasks = append([]Task(nil), q.tasks[q.head:]...)
		q.head = 0
	}
	return t, true
}

func (q *TaskQueue) Close() {
	q.mu.Lock()
	q.done = true
	q.cond.Broadcast()
	q.mu.Unlock()
}

func (q *TaskQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks) - q.head
}

func (q *TaskQueue) IdleWorkers() int64 {
	return atomic.LoadInt64(&q.idleWorkers)
}

// DrainRemaining returns all remaining tasks in the queue (used for pause/resume)
func (q *TaskQueue) DrainRemaining() []Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.head >= len(q.tasks) {
		return nil
	}

	remaining := make([]Task, len(q.tasks)-q.head)
	copy(remaining, q.tasks[q.head:])
	q.tasks = nil
	q.head = 0
	return remaining
}

// SplitLargestIfNeeded finds the largest queued task and splits it if > 2*MinChunk
// Returns true if a split occurred
func (q *TaskQueue) SplitLargestIfNeeded() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Find largest queued task
	idx := -1
	var maxLen int64 = 0
	for i, t := range q.tasks {
		if t.Length > maxLen && t.Length > 2*MinChunk {
			maxLen = t.Length
			idx = i
		}
	}

	if idx == -1 {
		return false // No task large enough to split
	}

	t := q.tasks[idx]

	// Split in half, aligned to AlignSize
	half := (t.Length / 2 / AlignSize) * AlignSize
	if half < MinChunk {
		return false // Halves would be too small
	}

	left := Task{Offset: t.Offset, Length: half}
	right := Task{Offset: t.Offset + half, Length: t.Length - half}

	// Replace original with right half, append left half
	q.tasks[idx] = right
	q.tasks = append(q.tasks, left)

	// Wake up idle workers
	q.cond.Broadcast()
	return true
}

// getInitialConnections returns the starting number of connections based on file size
func getInitialConnections(fileSize int64) int {
	switch {
	case fileSize < 10*MB:
		return 1
	case fileSize < 100*MB:
		return 4
	case fileSize < 1*GB:
		return 6
	default:
		return 16
	}
}

// calculateChunkSize determines optimal chunk size
func calculateChunkSize(fileSize int64, numConns int) int64 {
	targetChunks := int64(numConns * TasksPerWorker)
	chunkSize := fileSize / targetChunks

	// Clamp to min/max
	if chunkSize < MinChunk {
		chunkSize = MinChunk
	}
	if chunkSize > MaxChunk {
		chunkSize = MaxChunk
	}

	// Align to 4KB
	chunkSize = (chunkSize / AlignSize) * AlignSize
	if chunkSize == 0 {
		chunkSize = AlignSize
	}

	return chunkSize
}

// createTasks generates initial task queue from file size and chunk size
func createTasks(fileSize, chunkSize int64) []Task {
	var tasks []Task
	for offset := int64(0); offset < fileSize; offset += chunkSize {
		length := chunkSize
		if offset+length > fileSize {
			length = fileSize - offset
		}
		tasks = append(tasks, Task{Offset: offset, Length: length})
	}
	return tasks
}

// newConcurrentClient creates an http.Client tuned for concurrent downloads
func newConcurrentClient(numConns int) *http.Client {
	// Ensure we have enough connections per host
	maxConns := PerHostMax
	if numConns > maxConns {
		maxConns = numConns
	}

	transport := &http.Transport{
		// Connection pooling
		MaxIdleConns:        DefaultMaxIdleConns,
		MaxIdleConnsPerHost: maxConns + 2, // Slightly more than max to handle bursts
		MaxConnsPerHost:     maxConns,

		// Timeouts to prevent hung connections
		IdleConnTimeout:       DefaultIdleConnTimeout,
		TLSHandshakeTimeout:   DefaultTLSHandshakeTimeout,
		ResponseHeaderTimeout: DefaultResponseHeaderTimeout,
		ExpectContinueTimeout: DefaultExpectContinueTimeout,

		// Performance tuning
		DisableCompression: true,  // Files are usually already compressed
		ForceAttemptHTTP2:  false, // FORCE HTTP/1.1 for multiple TCP connections
		TLSNextProto:       make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),

		// Dial settings for TCP reliability
		DialContext: (&net.Dialer{
			Timeout:   DialTimeout,
			KeepAlive: KeepAliveDuration,
		}).DialContext,
	}

	return &http.Client{
		Transport: transport,
	}
}

// Download downloads a file using multiple concurrent connections
// Uses pre-probed metadata (file size already known)
func (d *ConcurrentDownloader) Download(ctx context.Context, rawurl, destPath string, fileSize int64, verbose bool) error {
	utils.Debug("ConcurrentDownloader.Download: %s -> %s (size: %d)", rawurl, destPath, fileSize)

	// Store URL and path for pause/resume
	d.URL = rawurl
	d.DestPath = destPath

	// Create cancellable context for pause support
	downloadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if d.State != nil {
		d.State.CancelFunc = cancel
	}

	// Determine connections and chunk size
	numConns := getInitialConnections(fileSize)
	chunkSize := calculateChunkSize(fileSize, numConns)

	// Create tuned HTTP client for concurrent downloads
	client := newConcurrentClient(numConns)

	if verbose {
		fmt.Printf("File size: %s, connections: %d, chunk size: %s\n",
			utils.ConvertBytesToHumanReadable(fileSize),
			numConns,
			utils.ConvertBytesToHumanReadable(chunkSize))
	}

	// Create and preallocate output file (use OpenFile to allow resume)
	outFile, err := os.OpenFile(destPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer outFile.Close()

	// Preallocate file to avoid fragmentation
	if err := outFile.Truncate(fileSize); err != nil {
		return fmt.Errorf("failed to preallocate file: %w", err)
	}

	// Create task queue - check for saved state first (resume case)
	var tasks []Task
	savedState, err := LoadState(destPath, rawurl)
	if err == nil && savedState != nil && len(savedState.Tasks) > 0 {
		// Resume: use saved tasks and reset downloaded counter
		tasks = savedState.Tasks
		if d.State != nil {
			d.State.Downloaded.Store(savedState.Downloaded)
		}
		utils.Debug("Resuming from saved state: %d tasks, %d bytes downloaded", len(tasks), savedState.Downloaded)
	} else {
		// Fresh download: create new tasks
		tasks = createTasks(fileSize, chunkSize)
	}
	queue := NewTaskQueue()
	queue.PushMultiple(tasks)

	// Start time for stats
	startTime := time.Now()

	// Start balancer goroutine for dynamic chunk splitting
	balancerCtx, cancelBalancer := context.WithCancel(downloadCtx)
	defer cancelBalancer()

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		maxSplits := 50
		splitCount := 0

		for {
			select {
			case <-balancerCtx.Done():
				return
			case <-ticker.C:
				if queue.IdleWorkers() > 0 && splitCount < maxSplits {
					if queue.SplitLargestIfNeeded() {
						splitCount++
						utils.Debug("Balancer: split largest task (total splits: %d)", splitCount)
					} else if queue.Len() == 0 {
						// Try to steal from an active worker
						if d.StealWork(queue) {
							splitCount++
						}
					}
				}
			}
		}
	}()

	// Monitor for completion
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-balancerCtx.Done():
				return
			case <-ticker.C:
				if queue.Len() == 0 && int(queue.IdleWorkers()) == numConns {
					queue.Close()
					return
				}
			}
		}
	}()

	// Start workers
	var wg sync.WaitGroup
	workerErrors := make(chan error, numConns)

	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			err := d.worker(downloadCtx, workerID, rawurl, outFile, queue, fileSize, startTime, verbose, client)
			if err != nil && err != context.Canceled {
				workerErrors <- err
			}
		}(i)
	}

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(workerErrors)
		queue.Close()
	}()

	// Check for errors or pause
	var downloadErr error
	for err := range workerErrors {
		if err != nil {
			downloadErr = err
		}
	}

	// Handle pause: save state and exit gracefully
	if d.State != nil && d.State.IsPaused() {
		// Collect remaining tasks
		remainingTasks := queue.DrainRemaining()

		// Also collect active tasks as remaining work
		d.activeMu.Lock()
		for _, active := range d.activeTasks {
			current := atomic.LoadInt64(&active.CurrentOffset)
			stopAt := atomic.LoadInt64(&active.StopAt)
			if current < stopAt {
				remainingTasks = append(remainingTasks, Task{
					Offset: current,
					Length: stopAt - current,
				})
			}
		}
		d.activeMu.Unlock()

		// Save state for resume
		state := &DownloadState{
			URLHash:    URLHash(d.URL),
			URL:        d.URL,
			DestPath:   destPath,
			TotalSize:  fileSize,
			Downloaded: d.State.Downloaded.Load(),
			Tasks:      remainingTasks,
			Filename:   filepath.Base(destPath),
		}
		if err := SaveState(destPath, d.URL, state); err != nil {
			utils.Debug("Failed to save pause state: %v", err)
		}

		utils.Debug("Download paused, state saved")
		return nil // Graceful exit, not an error
	}

	if downloadErr != nil {
		return downloadErr
	}

	// Final sync
	if err := outFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	// Delete state file on successful completion
	_ = DeleteState(destPath, d.URL)

	// Print final stats only if download wasn't cancelled
	if ctx.Err() == nil {
		elapsed := time.Since(startTime)
		speed := float64(fileSize) / elapsed.Seconds()
		fmt.Fprintf(os.Stderr, "\nDownloaded %s in %s (%s/s)\n",
			utils.ConvertBytesToHumanReadable(fileSize),
			elapsed.Round(time.Millisecond),
			utils.ConvertBytesToHumanReadable(int64(speed)))
	}

	return nil
}

// worker downloads tasks from the queue
func (d *ConcurrentDownloader) worker(ctx context.Context, id int, rawurl string, file *os.File, queue *TaskQueue, totalSize int64, startTime time.Time, verbose bool, client *http.Client) error {
	// Get pooled buffer
	bufPtr := bufPool.Get().(*[]byte)
	defer bufPool.Put(bufPtr)
	buf := *bufPtr

	utils.Debug("Worker %d started", id)
	defer utils.Debug("Worker %d finished", id)

	for {
		// Get next task
		task, ok := queue.Pop()

		if !ok {
			return nil // Queue closed, no more work
		}

		// Update active workers
		if d.State != nil {
			d.State.ActiveWorkers.Add(1)
		}

		var lastErr error
		for attempt := 0; attempt < maxTaskRetries; attempt++ {
			if attempt > 0 {
				time.Sleep(time.Duration(1<<attempt) * retryBaseDelay) //Exponential backoff incase of failure
			}

			// Register active task
			activeTask := &ActiveTask{
				Task:          task,
				CurrentOffset: task.Offset,
				StopAt:        task.Offset + task.Length,
			}
			d.activeMu.Lock()
			d.activeTasks[id] = activeTask
			d.activeMu.Unlock()

			taskStart := time.Now()
			lastErr = d.downloadTask(ctx, rawurl, file, activeTask, buf, verbose, client)
			utils.Debug("Worker %d: Task offset=%d length=%d took %v", id, task.Offset, task.Length, time.Since(taskStart))

			d.activeMu.Lock()
			delete(d.activeTasks, id)
			d.activeMu.Unlock()

			if lastErr == nil {
				// Check if we stopped early due to stealing
				stopAt := atomic.LoadInt64(&activeTask.StopAt)
				current := atomic.LoadInt64(&activeTask.CurrentOffset)
				if current < task.Offset+task.Length && current >= stopAt {
					// We were stopped early this is expected success for the partial work
					// The stolen part is already in the queue
				}
				break
			}

			if ctx.Err() != nil {
				if d.State != nil {
					d.State.ActiveWorkers.Add(-1)
				}
				return ctx.Err()
			}
		}

		// Update active workers
		if d.State != nil {
			d.State.ActiveWorkers.Add(-1)
		}

		if lastErr != nil {
			// Log failed task but continue with next task
			// If we modified StopAt we should probably reset it or push the remaining part?
			// TODO: Could optimize by pushing only remaining part if we track that.
			queue.Push(task)
			utils.Debug("task at offset %d failed after %d retries: %v", task.Offset, maxTaskRetries, lastErr)
		}
	}
}

// downloadTask downloads a single byte range and writes to file at offset
func (d *ConcurrentDownloader) downloadTask(ctx context.Context, rawurl string, file *os.File, activeTask *ActiveTask, buf []byte, verbose bool, client *http.Client) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawurl, nil)
	if err != nil {
		return err
	}

	task := activeTask.Task

	// TODO
	// We only request up to StopAt initially but StopAt might change during download
	// Actually, we should request the full length to keep connection open,
	// However, if we request full length, and stop halfway, the server might complain or we waste bandwidth?
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "+
		"AppleWebKit/537.36 (KHTML, like Gecko) "+
		"Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", task.Offset, task.Offset+task.Length-1))

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	// Read and write at offset
	offset := task.Offset
	for {
		// Check if we should stop
		stopAt := atomic.LoadInt64(&activeTask.StopAt)
		if offset >= stopAt {
			// Stealing happened, stop here
			return nil
		}

		// Calculate how much to read to fill buffer or hit stopAt/EOF
		// We want to fill buf as much as possible to minimize WriteAt calls

		// Limit by remaining length to stopAt
		remaining := stopAt - offset
		if remaining <= 0 {
			return nil
		}

		readSize := int64(len(buf))
		if readSize > remaining {
			readSize = remaining
		}

		readSoFar := 0
		var readErr error

		for readSoFar < int(readSize) {
			n, err := resp.Body.Read(buf[readSoFar:readSize])
			if n > 0 {
				readSoFar += n
			}
			if err != nil {
				readErr = err
				break
			}
		}

		if readSoFar > 0 {
			_, writeErr := file.WriteAt(buf[:readSoFar], offset)
			if writeErr != nil {
				return fmt.Errorf("write error: %w", writeErr)
			}
			offset += int64(readSoFar)
			atomic.StoreInt64(&activeTask.CurrentOffset, offset)

			// Update progress via shared state only (removed duplicate tracking)
			if d.State != nil {
				d.State.Downloaded.Add(int64(readSoFar))
			}
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read error: %w", readErr)
		}
	}

	return nil
}

// StealWork tries to split an active task from a busy worker
// It greedily targets the worker with the MOST remaining work.
func (d *ConcurrentDownloader) StealWork(queue *TaskQueue) bool {
	d.activeMu.Lock()
	defer d.activeMu.Unlock()

	var bestID int = -1
	var maxRemaining int64 = 0
	var bestActive *ActiveTask

	// Find the worker with the MOST remaining work
	for id, active := range d.activeTasks {
		current := atomic.LoadInt64(&active.CurrentOffset)
		stopAt := atomic.LoadInt64(&active.StopAt)
		remaining := stopAt - current

		if remaining > MinChunk && remaining > maxRemaining {
			maxRemaining = remaining
			bestID = id
			bestActive = active
		}
	}

	if bestID == -1 {
		return false
	}

	// Found the best candidate, now try to steal
	remaining := maxRemaining
	active := bestActive

	// Split in half
	splitSize := remaining / 2
	// Align to 4KB
	splitSize = (splitSize / AlignSize) * AlignSize

	if splitSize < MinChunk {
		return false
	}

	current := atomic.LoadInt64(&active.CurrentOffset)
	newStopAt := current + splitSize

	// Update the active task stop point
	atomic.StoreInt64(&active.StopAt, newStopAt)

	finalCurrent := atomic.LoadInt64(&active.CurrentOffset)

	// The actual start of the stolen chunk must be after where the worker effectively stops.
	stolenStart := newStopAt
	if finalCurrent > newStopAt {
		stolenStart = finalCurrent
	}

	// Double check: ensure we didn't race and lose the chunk
	currentStopAt := atomic.LoadInt64(&active.StopAt)
	if stolenStart >= currentStopAt && currentStopAt != newStopAt {
	}

	originalEnd := current + remaining

	if stolenStart >= originalEnd {
		return false
	}

	stolenTask := Task{
		Offset: stolenStart,
		Length: originalEnd - stolenStart,
	}

	queue.Push(stolenTask)
	utils.Debug("Balancer: stole %s from worker %d (new range: %d-%d)",
		utils.ConvertBytesToHumanReadable(stolenTask.Length), bestID, stolenTask.Offset, stolenTask.Offset+stolenTask.Length)

	return true
}
