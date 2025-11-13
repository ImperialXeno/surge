package downloader

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"surge/internal/utils"
	"sync"
	"time"
)

const (
	InitialSegments       = 8
	MaxWorkers            = 128
	DynamicWorkerInterval = 200 * time.Millisecond // polling rate for new worker creation
	MinSegmentSize        = 2 * 1024 * 1024        // 2 MB
	ProgressReporting     = 250 * time.Millisecond
)

type Segment struct {
	ID         int
	Start      int64
	End        int64
	Downloaded int64
	mu         sync.Mutex
	File       *os.File
}

func (s *Segment) Remaining() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.End - s.Start + 1 - s.Downloaded
}

type Worker struct {
	ID     int
	Client *http.Client
	wg     *sync.WaitGroup
}

/*
This function tries to create a newWorker
It tries to make a range request 0-0
and if resp code is 200 or 206
we consider that a success
and add this worker to pool
*/
func (d *Downloader) newWorker(parentCtx context.Context, rawurl string, workers *[]*Worker, workersMu *sync.Mutex, wg *sync.WaitGroup, segmentChan chan *Segment, verbose bool) (bool, error) {

	probeCtx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, rawurl, nil)
	if err != nil {
		return false, err
	}

	req.Header.Set("Range", "bytes=0-0")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "+
		"AppleWebKit/537.36 (KHTML, like Gecko) "+
		"Chrome/120.0.0.0 Safari/537.36") // We set a browser like header to avoid being blocked by some websites
	req.Header.Set("Connection", "close") // Asks server to close connection after request

	resp, err := d.Client.Do(req)
	if err != nil {
		return false, err
	}

	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return false, nil
	}

	workersMu.Lock()
	newWorkerID := len(*workers)
	worker := &Worker{ID: newWorkerID, Client: d.Client, wg: wg}
	*workers = append(*workers, worker)
	wg.Add(1)
	go worker.start(parentCtx, rawurl, segmentChan, verbose)
	workersMu.Unlock()

	if verbose {
		fmt.Fprintf(os.Stderr, "\n[probe] created new worker id=%d\n", newWorkerID)
	}
	return true, nil
}

func (d *Downloader) concurrentDownload(ctx context.Context, rawurl, outPath string, concurrent bool, verbose bool, md5sum, sha256sum string) error {

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, rawurl, nil)
	if err != nil {
		return err
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "+
		"AppleWebKit/537.36 (KHTML, like Gecko) "+
		"Chrome/120.0.0.0 Safari/537.36") // We set a browser like header to avoid being blocked by some websites

	resp, err := d.Client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.Header.Get("Accept-Ranges") != "bytes" {
		fmt.Println("Server does not support concurrent download, falling back to single thread")
		return d.singleDownload(ctx, rawurl, outPath, verbose, md5sum, sha256sum)
	}

	filename, _, err := utils.DetermineFilename(rawurl, resp, verbose)
	if err != nil {
		return err
	}

	totalSize, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return err
	}

	tmpDir := filepath.Join(filepath.Dir(outPath), fmt.Sprintf("%s-surge", filename))
	err = os.Mkdir(tmpDir, 0o755)
	if err != nil {
		return err
	}

	var segmentsMu sync.Mutex
	segments := make([]*Segment, InitialSegments)
	segmentSize := totalSize / InitialSegments
	for i := 0; i < InitialSegments; i++ {

		start := int64(i) * segmentSize
		end := start + segmentSize - 1

		if i == InitialSegments-1 {
			end = totalSize
		}

		partFileName := filepath.Join(tmpDir, fmt.Sprintf("%s.part%d", filename, i))
		file, err := os.Create(partFileName)
		if err != nil {
			return err
		}

		segments[i] = &Segment{ID: i, Start: start, End: end, File: file}
	}

	var wg sync.WaitGroup
	segmentChan := make(chan *Segment, MaxWorkers)
	for _, s := range segments {
		segmentChan <- s
	}

	var workersMu sync.Mutex
	workers := make([]*Worker, 0, MaxWorkers)
	for i := 0; i < InitialSegments; i++ {
		wg.Add(1)
		worker := &Worker{ID: i, Client: d.Client, wg: &wg}
		workers = append(workers, worker)
		go worker.start(ctx, rawurl, segmentChan, verbose)
	}

	startTime := time.Now()
	var totalDownloaded int64

	go func() {
		for {
			currentDownloaded := int64(0)
			segmentsMu.Lock()
			for _, s := range segments {
				s.mu.Lock()
				currentDownloaded += s.Downloaded
				s.mu.Unlock()
			}
			segmentsMu.Unlock()

			totalDownloaded = currentDownloaded
			d.printProgress(totalDownloaded, totalSize, startTime, verbose)
			if totalDownloaded >= totalSize {
				return
			}

			time.Sleep(ProgressReporting)
		}
	}()

	go func() {

		ticker := time.NewTicker(DynamicWorkerInterval)
		defer ticker.Stop()
		for range ticker.C {

			if totalDownloaded >= totalSize {
				return
			}

			workersMu.Lock()
			newWorkerCreated, err := d.newWorker(ctx, rawurl, &workers, &workersMu, &wg, segmentChan, verbose)

			if err != nil || !newWorkerCreated {
				continue
			}

			// Worker has been created, find largest segment to split
			segmentsMu.Lock()
			sort.Slice(segments, func(i, j int) bool {
				return segments[i].Remaining() > segments[j].Remaining()
			})
			largestSegment := segments[0]

			if largestSegment.Remaining() < MinSegmentSize {
				segmentsMu.Unlock()
				continue
			}

			largestSegment.mu.Lock()
			midpoint := largestSegment.Start + largestSegment.Downloaded + largestSegment.Remaining()/2
			newSegmentEnd := largestSegment.End
			largestSegment.End = midpoint
			largestSegment.mu.Unlock()

			newID := len(segments)
			partFileName := filepath.Join(tmpDir, fmt.Sprintf("%s.part%d", filename, newID))
			file, err := os.Create(partFileName)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating file for new segment: %v\n", err)
				segmentsMu.Unlock()
				continue
			}

			newSegment := &Segment{ID: newID, Start: midpoint + 1, End: newSegmentEnd, File: file}
			segments = append(segments, newSegment)
			segmentChan <- newSegment
			segmentsMu.Unlock()

			if verbose {
				fmt.Fprintf(os.Stderr, "\n[split] worker id=%d split segment id=%d into new segment id=%d\n",
					workers[len(workers)-1].ID, largestSegment.ID, newSegment.ID)
			}
		}
	}()

	wg.Wait()
	close(segmentChan)

	d.printProgress(totalDownloaded, totalSize, startTime, verbose)

	destPath := outPath
	if info, err := os.Stat(outPath); err == nil && info.IsDir() {
		destPath = filepath.Join(outPath, filename)
	}

	destFile, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer destFile.Close()

	segmentsMu.Lock()
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].ID < segments[j].ID
	})

	for _, s := range segments {
		pathFileName := filepath.Join(tmpDir, fmt.Sprintf("%s.part%d", filename, s.ID))
		partFile, err := os.Open(pathFileName)
		if err != nil {
			segmentsMu.Unlock()
			return err
		}

		_, err = io.Copy(destFile, partFile)
		partFile.Close()
		if err != nil {
			segmentsMu.Unlock()
			return err
		}

		os.Remove(pathFileName)
	}
	segmentsMu.Unlock()

	file, err := os.Open(destPath)
	if err != nil {
		return fmt.Errorf("failed to open merged file for checksum verification: %w", err)
	}
	defer file.Close()

	serverMD5 := resp.Header.Get("Content-MD5")
	serverSHA256 := resp.Header.Get("X-Checksum-SHA256")
	if err := utils.VerifyChecksum(file, md5sum, sha256sum, serverMD5, serverSHA256, verbose); err != nil {
		return err
	}

	elapsed := time.Since(startTime)
	speed := float64(totalSize) / 1024.0 / elapsed.Seconds()
	fmt.Fprintf(os.Stderr, "\nDownloaded %s in %s (%s/s)\n", destPath, elapsed.Round(time.Second), utils.ConvertBytesToHumanReadable(int64(speed*1024)))
	return nil

}

func (w *Worker) start(ctx context.Context, rawurl string, segmentChan <-chan *Segment, verbose bool) {
	defer w.wg.Done()
	for segment := range segmentChan {
		err := w.downloadSegment(ctx, rawurl, segment, verbose)
		if err != nil && verbose {
			fmt.Fprintf(os.Stderr, "\n[worker %d] error downloading segment %d: %v\n", w.ID, segment.ID, err)
			// Requeue the segment for another attempt
			// Delete chunk file to avoid appending to corrupted data
			// segment.File.Close()
			// partFileName := segment.File.Name()
			// os.Remove(partFileName)
			// newFile, err := os.Create(partFileName)
			// if err != nil {
			// 	fmt.Fprintf(os.Stderr, "\n[worker %d] error recreating file for segment %d: %v\n", w.ID, segment.ID, err)
			// 	continue
			// }
			// segment.File = newFile
			// segment.Downloaded = 0
			// segmentChan <- segment
		}
	}
}

func (w *Worker) downloadSegment(ctx context.Context, rawurl string, segment *Segment, verbose bool) error {

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawurl, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", segment.Start+segment.Downloaded, segment.End))
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "+
		"AppleWebKit/537.36 (KHTML, like Gecko) "+
		"Chrome/120.0.0.0 Safari/537.36") // We set a browser like header to avoid being blocked by some websites

	req.Header.Set("Connection", "close") // Asks server to close connection after request

	resp, err := w.Client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code %d for segment %d", resp.StatusCode, segment.ID)
	}

	buffer := make([]byte, 32*1024)
	for {
		n, err := resp.Body.Read(buffer)
		if n > 0 {
			_, writeErr := segment.File.Write(buffer[:n])
			if writeErr != nil {
				return writeErr
			}
			segment.mu.Lock()
			segment.Downloaded += int64(n)
			segment.mu.Unlock()
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
	}

	return segment.File.Close()
}

func (d *Downloader) printProgress(written, total int64, start time.Time, verbose bool) {
	elapsed := time.Since(start).Seconds()
	if elapsed == 0 {
		return
	}
	speed := float64(written) / 1024.0 / elapsed

	d.mu.Lock()
	d.bytesDownloadedPerSecond = append(d.bytesDownloadedPerSecond, int64(speed))
	if len(d.bytesDownloadedPerSecond) > 30 {
		d.bytesDownloadedPerSecond = d.bytesDownloadedPerSecond[1:]
	}

	var avgSpeed float64
	var totalSpeed int64
	for _, s := range d.bytesDownloadedPerSecond {
		totalSpeed += s
	}
	if len(d.bytesDownloadedPerSecond) > 0 {
		avgSpeed = float64(totalSpeed) / float64(len(d.bytesDownloadedPerSecond))
	}
	d.mu.Unlock()

	eta := "N/A"
	if total > 0 && avgSpeed > 0 {
		remainingBytes := total - written
		if remainingBytes < 0 {
			remainingBytes = 0
		}
		remainingSeconds := float64(remainingBytes) / (avgSpeed * 1024)
		eta = time.Duration(remainingSeconds * float64(time.Second)).Round(time.Second).String()
	}

	if total > 0 {
		pct := float64(written) / float64(total) * 100.0
		if pct > 100 {
			pct = 100
		}
		fmt.Fprintf(os.Stderr, "\r[SURGE] %.2f%% %s/%s (%.1f KiB/s) ETA: %s ", pct, utils.ConvertBytesToHumanReadable(written), utils.ConvertBytesToHumanReadable(total), avgSpeed, eta)
	} else {
		fmt.Fprintf(os.Stderr, "\r[SURGE] %s (%.1f KiB/s) ", utils.ConvertBytesToHumanReadable(written), avgSpeed)
	}
}
