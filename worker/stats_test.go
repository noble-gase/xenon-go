package worker

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
)

// blocker 返回一个阻塞到 release 关闭的任务，以及「已开始执行」的通知。
func blocker(n int) (job func(context.Context), started chan struct{}, unblock func()) {
	release := make(chan struct{})
	started = make(chan struct{}, n)
	job = func(context.Context) {
		started <- struct{}{}
		<-release
	}
	return job, started, sync.OnceFunc(func() { close(release) })
}

func TestStatsOnIdlePool(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := New(4, WithCacheSize(8))
		defer p.Close()

		s := p.Stats()
		assert.Equal(t, 0, s.Running)
		assert.Equal(t, 0, s.Pending)
		assert.Equal(t, 0, s.Waiting)
		assert.Equal(t, 0, s.Live)
		assert.Equal(t, 4, s.Capacity)
		assert.Equal(t, 8, s.CacheSize)
	})
}

func TestStatsReflectsRunningTasks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := New(2)
		defer p.Close()

		job, started, unblock := blocker(2)
		defer unblock()

		for range 2 {
			assert.NoError(t, p.Go(context.Background(), job))
			<-started
		}
		synctest.Wait()

		s := p.Stats()
		assert.Equal(t, 2, s.Running, "两个任务都在执行")
		assert.Equal(t, 0, s.Pending)
		assert.Equal(t, 2, s.Live)

		unblock()
		synctest.Wait()

		s = p.Stats()
		assert.Equal(t, 0, s.Running, "任务结束后应归零")
		assert.Equal(t, 2, s.Live, "协程仍存活等待复用")
	})
}

func TestStatsReflectsPendingBacklog(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := New(1, WithCacheSize(8))
		defer p.Close()

		job, started, unblock := blocker(5)
		defer unblock()

		assert.NoError(t, p.Go(context.Background(), job))
		<-started

		// 上限已满，后续任务只能进缓存
		for range 4 {
			assert.NoError(t, p.Go(context.Background(), job))
		}
		synctest.Wait()

		s := p.Stats()
		assert.Equal(t, 1, s.Running)
		assert.Equal(t, 4, s.Pending, "已接收但没有协程可执行")
		assert.Equal(t, 0, s.Waiting, "缓存未满，提交方不应阻塞")

		unblock()
		synctest.Wait()

		s = p.Stats()
		assert.Equal(t, 0, s.Running)
		assert.Equal(t, 0, s.Pending, "排空后不应残留")
	})
}

func TestStatsReflectsWaitingSubmitters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := New(1)
		defer p.Close()

		job, started, unblock := blocker(3)
		defer unblock()

		assert.NoError(t, p.Go(context.Background(), job))
		<-started

		// 无缓存：这个任务被 run 接收后卡在 deliver，input 从此无人读取
		assert.NoError(t, p.Go(context.Background(), job))
		synctest.Wait()
		assert.Equal(t, 0, p.Stats().Waiting, "已接收的提交方不算等待")

		// 再提交就只能阻塞在 Go 里
		var submitted atomic.Bool
		go func() {
			_ = p.Go(context.Background(), job)
			submitted.Store(true)
		}()
		synctest.Wait()

		s := p.Stats()
		assert.Equal(t, 1, s.Waiting, "提交方阻塞在 Go")
		assert.Equal(t, 1, s.Pending, "run 手上那个")
		assert.False(t, submitted.Load())

		unblock()
		synctest.Wait()
		assert.Equal(t, 0, p.Stats().Waiting, "放行后应归零")
		assert.True(t, submitted.Load())
	})
}

func TestStatsCountersDoNotDriftOnAbnormalExit(t *testing.T) {
	for _, tc := range []struct {
		name string
		job  func(context.Context)
	}{
		{name: "panic", job: func(context.Context) { panic("boom") }},
		{name: "goexit", job: func(context.Context) { runtime.Goexit() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := New(2, WithPanicHandler(func(context.Context, any, []byte) {}))
				defer p.Close()

				for range 4 {
					assert.NoError(t, p.Go(context.Background(), tc.job))
				}
				synctest.Wait()

				s := p.Stats()
				assert.Equal(t, 0, s.Running, "异常退出也要归还计数")
				assert.Equal(t, 0, s.Pending)
				assert.Equal(t, 0, s.Waiting)

				// 池仍可用
				done := make(chan struct{})
				assert.NoError(t, p.Go(context.Background(), func(context.Context) { close(done) }))
				<-done
				synctest.Wait()
				assert.Equal(t, 0, p.Stats().Running)
			})
		})
	}
}

func TestStatsAfterClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := New(2, WithCacheSize(4))

		var completed atomic.Int64
		for range 6 {
			assert.NoError(t, p.Go(context.Background(), func(context.Context) { completed.Add(1) }))
		}
		p.Close()
		synctest.Wait()

		s := p.Stats()
		assert.Equal(t, int64(6), completed.Load(), "关闭不丢任务")
		assert.Equal(t, 0, s.Running)
		assert.Equal(t, 0, s.Pending)
		assert.Equal(t, 0, s.Waiting)
		assert.Equal(t, 0, s.Live, "协程已全部退出")
	})
}
