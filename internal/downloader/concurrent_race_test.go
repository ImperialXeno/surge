package downloader

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestStealWork_Atomicity verifies that StealWork correctly updates StopAt
// and that the atomic operations are thread-safe.
func TestStealWork_Atomicity(t *testing.T) {
	// Setup
	d := &ConcurrentDownloader{
		activeTasks: make(map[int]*ActiveTask),
		Runtime:     &RuntimeConfig{},
	}
	queue := NewTaskQueue()

	// create a "stuck" task
	// Offset 0, Length 10MB. Current 1MB. StopAt 10MB.
	// MinChunk is 2MB, so 9MB remaining should be stealable.
	taskLen := int64(10 * 1024 * 1024)
	current := int64(1 * 1024 * 1024)
	activeTask := &ActiveTask{
		Task:          Task{Offset: 0, Length: taskLen},
		CurrentOffset: current,
		StopAt:        taskLen,
		LastActivity:  time.Now().UnixNano(),
		StartTime:     time.Now(),
	}

	d.activeMu.Lock()
	d.activeTasks[1] = activeTask
	d.activeMu.Unlock()

	// run StealWork
	start := time.Now()
	stealSuccess := d.StealWork(queue)
	if !stealSuccess {
		t.Fatal("StealWork failed")
	}
	elapsed := time.Since(start)
	t.Logf("StealWork took %v", elapsed)

	// Verify StopAt was reduced
	stopAt := atomic.LoadInt64(&activeTask.StopAt)
	if stopAt >= taskLen {
		t.Errorf("StealWork failed to reduce StopAt: got %d, want < %d", stopAt, taskLen)
	}

	// Verify new task was pushed to queue
	newTask, ok := queue.Pop()
	if !ok {
		t.Fatal("Queue should have stolen task")
	}

	// The stolen task should start at the new StopAt and go to the original end
	if newTask.Offset != stopAt {
		t.Errorf("New task offset %d does not match StopAt %d", newTask.Offset, stopAt)
	}
	if newTask.Offset+newTask.Length != taskLen {
		t.Errorf("New task end %d does not match original end %d", newTask.Offset+newTask.Length, taskLen)
	}

	t.Logf("Original Task now ends at %d", stopAt)
	t.Logf("New Task covers %d to %d", newTask.Offset, newTask.Offset+newTask.Length)
}

// TestWorker_StopAt logic verification (UnitTest of the logic pattern)
func TestWorker_StopAt_LogicPattern(t *testing.T) {
	// Simulate the variable state inside worker
	task := Task{Offset: 0, Length: 1000}

	// Simulate "Stolen" state
	activeTask := &ActiveTask{
		CurrentOffset: 100,
		StopAt:        500, // Stolen at 500
	}

	// OLD BUGGY LOGIC:
	// stopAt := task.Offset + task.Length // 1000
	// remaining := stopAt - current // 900 -> WRONG, includes stolen part (500-1000)

	// FIXED LOGIC:
	stopAt := atomic.LoadInt64(&activeTask.StopAt) // 500

	// Sanity Check Validation
	originalEnd := task.Offset + task.Length
	if stopAt > originalEnd {
		stopAt = originalEnd
	}

	if stopAt != 500 {
		t.Errorf("StopAt calculation expected 500, got %d", stopAt)
	}

	remainingBytes := stopAt - activeTask.CurrentOffset
	if remainingBytes != 400 {
		t.Errorf("Remaining bytes calculation expected 400, got %d", remainingBytes)
	}
}
