package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPoolExecutesSubmittedTasks(t *testing.T) {
	p := New(2)
	defer p.Close()

	var wg sync.WaitGroup
	var completed int32

	for range 20 {
		wg.Add(1)
		err := p.Go(context.Background(), func(context.Context) {
			defer wg.Done()
			atomic.AddInt32(&completed, 1)
		})
		assert.NoError(t, err)
	}

	waitGroup(t, &wg, time.Second)
	assert.Equal(t, int32(20), atomic.LoadInt32(&completed))
}

func TestPoolLimit(t *testing.T) {
	p := New(2)
	defer p.Close()

	var wg sync.WaitGroup
	var active int32
	var maxActive int32

	for range 12 {
		wg.Add(1)
		err := p.Go(context.Background(), func(context.Context) {
			defer wg.Done()

			current := atomic.AddInt32(&active, 1)
			for {
				max := atomic.LoadInt32(&maxActive)
				if current <= max || atomic.CompareAndSwapInt32(&maxActive, max, current) {
					break
				}
			}

			time.Sleep(10 * time.Millisecond)
			atomic.AddInt32(&active, -1)
		})
		assert.NoError(t, err)
	}

	waitGroup(t, &wg, time.Second)
	assert.LessOrEqual(t, atomic.LoadInt32(&maxActive), int32(2))
}

func TestGoReturnsContextErrorWhenBlocked(t *testing.T) {
	p := New(1)
	defer p.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	assert.NoError(t, p.Go(context.Background(), func(context.Context) {
		defer wg.Done()
		close(started)
		<-release
	}))

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first task did not start")
	}

	wg.Add(1)
	assert.NoError(t, p.Go(context.Background(), func(context.Context) {
		defer wg.Done()
		<-release
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := p.Go(ctx, func(context.Context) {
		t.Fatal("timed-out task should not run")
	})
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	close(release)
	waitGroup(t, &wg, time.Second)
}

func TestGoAfterCloseReturnsErrPoolClosed(t *testing.T) {
	p := New(1)
	p.Close()

	err := p.Go(context.Background(), func(context.Context) {
		t.Fatal("task should not run after Close")
	})
	assert.ErrorIs(t, err, ErrPoolClosed)
}

func TestRecover(t *testing.T) {
	recovered := make(chan any, 1)
	p := New(1, WithPanicHandler(func(ctx context.Context, err any, stack []byte) {
		recovered <- err
	}))
	defer p.Close()

	assert.NoError(t, p.Go(context.Background(), func(context.Context) {
		panic("boom")
	}))

	select {
	case err := <-recovered:
		assert.Equal(t, "boom", err)
	case <-time.After(time.Second):
		t.Fatal("panic handler was not called")
	}
}

func TestWorkerLRUSkipsBusyWorkers(t *testing.T) {
	lru := NewWorkerLRU()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &worker{id: 1, ctx: ctx, cancel: cancel}
	lru.Upsert(w)
	w.keepalive.Store(time.Now().Add(-time.Hour).UnixNano())
	w.busy.Store(true)

	lru.IdleCheck(time.Millisecond)
	assert.Equal(t, 1, lru.Len())

	w.busy.Store(false)
	lru.IdleCheck(time.Millisecond)
	assert.Equal(t, 0, lru.Len())

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("idle worker was not canceled")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	p := New(1)
	p.Close()
	p.Close()

	err := p.Go(context.Background(), func(context.Context) {})
	assert.True(t, errors.Is(err, ErrPoolClosed))
}

func waitGroup(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("timed out waiting for tasks")
	}
}
