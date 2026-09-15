package worker

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCachedTasksSurviveIdleReclaimWithoutNewSubmissions(t *testing.T) {
	for range 20 {
		p := New(1, WithCacheSize(64)).(*pool)
		stop, stopped := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(stopped)
			for {
				select {
				case <-stop:
					return
				default:
					p.workers.IdleCheck(0)
					runtime.Gosched()
				}
			}
		}()
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		cleanup := sync.OnceFunc(func() {
			close(stop)
			<-stopped
			unblock()
			p.Close()
		})
		t.Cleanup(cleanup)

		started := make(chan struct{})
		assert.NoError(t, p.Go(context.Background(), func(context.Context) {
			close(started)
			<-release
		}))
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("初始任务未执行")
		}
		var completed sync.WaitGroup
		completed.Add(64)
		for range 64 {
			assert.NoError(t, p.Go(context.Background(), func(context.Context) {
				defer completed.Done()
				runtime.Gosched()
			}))
		}
		unblock()
		// 此后没有新提交，回收信号必须能独立恢复全部缓存任务。
		waitGroup(t, &completed, 3*time.Second)
		cleanup()
	}
}

func TestConcurrentSubmissionAndShutdownExecutesAcceptedTasksOnce(t *testing.T) {
	for _, cache := range []int{0, 8} {
		p := New(4, WithCacheSize(cache))
		var callers sync.WaitGroup
		var accepted [128]bool
		var executed [128]atomic.Int64
		start := make(chan struct{})
		for i := range accepted {
			callers.Go(func() {
				<-start
				err := p.Go(context.Background(), func(context.Context) { executed[i].Add(1) })
				if err == nil {
					accepted[i] = true
				} else {
					assert.ErrorIs(t, err, ErrPoolClosed)
				}
			})
		}
		closed := make(chan error, 1)
		go func() {
			<-start
			closed <- p.Shutdown(context.Background())
		}()
		close(start)
		callers.Wait()
		assert.NoError(t, <-closed)
		for i, ok := range accepted {
			want := int64(0)
			if ok {
				want = 1
			}
			assert.Equal(t, want, executed[i].Load(), "cache=%d task=%d", cache, i)
		}
	}
}
