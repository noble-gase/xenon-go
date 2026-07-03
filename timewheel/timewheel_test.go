package timewheel

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/noble-gase/xenon/worker"
	"github.com/stretchr/testify/assert"
)

func TestTimeWheelRunsAndRetriesTask(t *testing.T) {
	tw := New(
		WithTimeLevel(Level(8, 10*time.Millisecond)),
		WithWorkerPool(worker.New(2, worker.WithCacheSize(4))),
	)
	defer tw.Stop()

	attempts := make(chan int, 3)
	tw.Go(context.Background(), "retry", func(ctx context.Context, task *Task) time.Duration {
		attempts <- task.Attempts()
		if task.Attempts() < 3 {
			return 10 * time.Millisecond
		}
		return 0
	}, time.Now())

	assert.Equal(t, 1, receiveInt(t, attempts, time.Second))
	assert.Equal(t, 2, receiveInt(t, attempts, time.Second))
	assert.Equal(t, 3, receiveInt(t, attempts, time.Second))
}

func TestTaskCancelCallsCancelFn(t *testing.T) {
	canceled := make(chan string, 1)
	tw := New(
		WithTimeLevel(Level(8, 20*time.Millisecond)),
		WithWorkerPool(worker.New(1, worker.WithCacheSize(2))),
		WithCancelFn(func(ctx context.Context, task *Task) {
			canceled <- task.ID()
		}),
	)
	defer tw.Stop()

	task := tw.Go(context.Background(), "cancel-me", func(ctx context.Context, task *Task) time.Duration {
		t.Fatal("canceled task should not run")
		return 0
	}, time.Now().Add(10*time.Millisecond))
	task.Cancel(context.Canceled)

	select {
	case id := <-canceled:
		assert.Equal(t, "cancel-me", id)
	case <-time.After(time.Second):
		t.Fatal("cancel handler was not called")
	}
}

func TestContextDonePreventsTaskExecution(t *testing.T) {
	canceled := make(chan string, 1)
	tw := New(
		WithTimeLevel(Level(8, 20*time.Millisecond)),
		WithWorkerPool(worker.New(1, worker.WithCacheSize(2))),
		WithCancelFn(func(ctx context.Context, task *Task) {
			canceled <- task.ID()
		}),
	)
	defer tw.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	tw.Go(ctx, "ctx-cancel", func(ctx context.Context, task *Task) time.Duration {
		t.Fatal("task with canceled context should not run")
		return 0
	}, time.Now().Add(10*time.Millisecond))
	cancel()

	select {
	case id := <-canceled:
		assert.Equal(t, "ctx-cancel", id)
	case <-time.After(time.Second):
		t.Fatal("cancel handler was not called")
	}
}

func TestStopPreventsPendingTaskExecution(t *testing.T) {
	tw := New(
		WithTimeLevel(Level(8, 20*time.Millisecond)),
		WithWorkerPool(worker.New(1, worker.WithCacheSize(2))),
	)

	var ran int32
	tw.Go(context.Background(), "pending", func(ctx context.Context, task *Task) time.Duration {
		atomic.AddInt32(&ran, 1)
		return 0
	}, time.Now().Add(30*time.Millisecond))
	tw.Stop()

	time.Sleep(80 * time.Millisecond)
	assert.Equal(t, int32(0), atomic.LoadInt32(&ran))
}

func TestBucketResetReturnsPreviousTasks(t *testing.T) {
	bucket := NewBucket()
	task := &Task{id: "task-1"}
	bucket.Add(task)

	old := bucket.Reset()
	assert.Equal(t, 1, old.Len())
	assert.Same(t, task, old.Front().Value)
	assert.Equal(t, 0, bucket.Reset().Len())
}

func receiveInt(t *testing.T, ch <-chan int, timeout time.Duration) int {
	t.Helper()

	select {
	case v := <-ch:
		return v
	case <-time.After(timeout):
		t.Fatal("timed out waiting for value")
		return 0
	}
}
