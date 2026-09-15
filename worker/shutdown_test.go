package worker

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
)

// 使用虚拟时钟验证期限，避免依赖测试机器的调度速度。
func assertShutdownTimeout(t *testing.T, p Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- p.Shutdown(ctx) }()
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-result:
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	default:
		t.Error("Shutdown 未按当前调用的期限返回")
	}
}

func TestShutdownPreservesPendingAndCachedTasks(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cache   int
		pending bool
	}{
		{name: "no_cache", pending: true},
		{name: "full_cache", cache: 8, pending: true},
		{name: "cached_only", cache: 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := New(1, WithCacheSize(tc.cache))
				defer p.Close()
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				started := make(chan struct{}, tc.cache+2)
				var active, maximum, completed atomic.Int64
				job := func(context.Context) {
					n := active.Add(1)
					for old := maximum.Load(); n > old; old = maximum.Load() {
						if maximum.CompareAndSwap(old, n) {
							break
						}
					}
					started <- struct{}{}
					<-release
					active.Add(-1)
					completed.Add(1)
				}
				assert.NoError(t, p.Go(context.Background(), job))
				<-started
				queued := tc.cache
				if tc.pending {
					queued++ // 填满缓存，再留一个已接收的任务在调度协程手上。
				}
				for range queued {
					assert.NoError(t, p.Go(context.Background(), job))
				}
				assertShutdownTimeout(t, p)
				assert.ErrorIs(t, p.Go(context.Background(), job), ErrPoolClosed)
				unblock()
				assert.NoError(t, p.Shutdown(context.Background()))
				synctest.Wait()
				assert.Equal(t, int64(queued+1), completed.Load())
				assert.Equal(t, int64(1), maximum.Load(), "关闭期间不能突破并发上限")
			})
		})
	}
}

func TestConcurrentShutdownUsesIndependentContexts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := New(1)
		defer p.Close()
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		assert.NoError(t, p.Go(context.Background(), func(context.Context) { <-release }))
		synctest.Wait()

		first := make(chan error, 1)
		go func() { first <- p.Shutdown(context.Background()) }()
		synctest.Wait()
		assertShutdownTimeout(t, p)
		select {
		case <-first:
			t.Error("首次关闭在任务结束前返回")
		default:
		}
		unblock()
		assert.NoError(t, <-first)
		assert.NoError(t, p.Shutdown(context.Background()))
	})
}

func TestCloseTimeoutContinuesDraining(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := New(1, WithCloseTimeout(20*time.Millisecond))
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		var completed atomic.Int64
		for range 2 {
			assert.NoError(t, p.Go(context.Background(), func(context.Context) {
				<-release
				completed.Add(1)
			}))
		}
		closed := make(chan struct{})
		go func() { p.Close(); close(closed) }()
		time.Sleep(100 * time.Millisecond)
		select {
		case <-closed:
		default:
			t.Error("Close 未按 WithCloseTimeout 返回")
		}
		unblock()
		assert.NoError(t, p.Shutdown(context.Background()))
		assert.Equal(t, int64(2), completed.Load())
	})
}

func TestWorkerExitRefillsCacheWithAnotherWorkerBusy(t *testing.T) {
	for _, name := range []string{"running", "draining"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := New(2, WithCacheSize(8))
				defer p.Close()
				busy, retiring := make(chan struct{}), make(chan struct{})
				unblockBusy := sync.OnceFunc(func() { close(busy) })
				unblockRetiring := sync.OnceFunc(func() { close(retiring) })
				defer unblockBusy()
				defer unblockRetiring()
				assert.NoError(t, p.Go(context.Background(), func(context.Context) { <-busy }))
				assert.NoError(t, p.Go(context.Background(), func(context.Context) {
					<-retiring
					runtime.Goexit()
				}))
				synctest.Wait()
				var completed atomic.Int64
				for range 8 {
					assert.NoError(t, p.Go(context.Background(), func(context.Context) { completed.Add(1) }))
				}
				synctest.Wait()
				if name == "draining" {
					assertShutdownTimeout(t, p)
				}
				unblockRetiring()
				synctest.Wait()
				assert.Equal(t, int64(8), completed.Load(), "没有新提交时也要补充退出的 worker")
				unblockBusy()
				assert.NoError(t, p.Shutdown(context.Background()))
			})
		})
	}
}

func TestShutdownPreservesCancellationCause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := New(1)
		release := make(chan struct{})
		assert.NoError(t, p.Go(context.Background(), func(context.Context) { <-release }))
		synctest.Wait()
		ctx, cancel := context.WithCancelCause(context.Background())
		cause := context.DeadlineExceeded
		cancel(cause)
		assert.ErrorIs(t, p.Shutdown(ctx), cause)
		close(release)
		assert.NoError(t, p.Shutdown(context.Background()))
		assert.NoError(t, p.Shutdown(ctx), "已完成的关闭应返回成功")
	})
}
