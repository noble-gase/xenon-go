package worker

import (
	"context"
	"errors"
	"runtime"
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

// 回收把协程清空后，run 必须能补充协程继续投递，而不是永久阻塞。
// 用极限回收（IdleCheck(0)）放大 run 通过容量检查与协程被回收之间的窗口。
func TestPoolRecoversWhenAllWorkersReclaimed(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	p := New(1, WithIdleTimeout(time.Millisecond)).(*pool)
	defer p.Close()

	stop := make(chan struct{})
	defer close(stop)
	go func() {
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

	var executed, submitted atomic.Int64
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			submitted.Add(1)
			if err := p.Go(context.Background(), func(context.Context) { executed.Add(1) }); err != nil {
				return
			}
		}
	}()

	last := int64(0)
	for range 30 {
		time.Sleep(100 * time.Millisecond)
		cur := executed.Load()
		if cur == last && submitted.Load() > cur {
			t.Fatalf("Pool 卡死: 已提交=%d 已执行=%d live=%d", submitted.Load(), cur, p.live.Load())
		}
		last = cur
	}
}

// 极限回收下存活计数不能泄漏或变负，且 Close 后必须归零
func TestLiveCountStaysBoundedUnderReclaim(t *testing.T) {
	p := New(4, WithIdleTimeout(time.Millisecond)).(*pool)

	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				p.workers.IdleCheck(0)
			}
		}
	}()

	for i := range 20000 {
		done := make(chan struct{})
		assert.NoError(t, p.Go(context.Background(), func(context.Context) { close(done) }))

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatalf("第 %d 个任务未执行, live=%d", i, p.live.Load())
		}

		if live := p.live.Load(); live < 0 || live > 4 {
			t.Fatalf("live 越界: %d", live)
		}
	}

	close(stop)
	p.Close()

	assert.Eventually(t, func() bool { return p.live.Load() == 0 },
		time.Second, 10*time.Millisecond, "Close 后仍有协程未退出")
}

// panicFn 自身 panic 不能打穿工作协程（否则会终止整个进程）
func TestPanicHandlerPanicDoesNotKillWorker(t *testing.T) {
	p := New(1, WithPanicHandler(func(context.Context, any, []byte) {
		panic("handler exploded")
	}))
	defer p.Close()

	for range 5 {
		assert.NoError(t, p.Go(context.Background(), func(context.Context) { panic("boom") }))
	}

	done := make(chan struct{})
	assert.NoError(t, p.Go(context.Background(), func(context.Context) { close(done) }))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("panicFn 崩溃后 Pool 不可用")
	}
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

// Close 必须等待执行中的任务结束，否则 SIGTERM 时会截断任务
func TestCloseWaitsForInflightTasks(t *testing.T) {
	p := New(1)

	started := make(chan struct{})
	var finished atomic.Bool

	assert.NoError(t, p.Go(context.Background(), func(context.Context) {
		close(started)
		time.Sleep(300 * time.Millisecond)
		finished.Store(true)
	}))
	<-started

	p.Close()
	assert.True(t, finished.Load(), "Close 返回时执行中的任务尚未结束")
}

// Close 时堆积的缓存任务由工作协程并发排空，而不是压在调用方协程上串行执行
func TestCloseDrainsCacheConcurrently(t *testing.T) {
	const (
		workers = 8
		cached  = 24
		cost    = 100 * time.Millisecond
	)

	p := New(workers, WithCacheSize(64))

	// 占满全部工作协程
	block := make(chan struct{})
	for range workers {
		assert.NoError(t, p.Go(context.Background(), func(context.Context) { <-block }))
	}

	// 其余任务堆进缓存
	var executed atomic.Int64
	for range cached {
		assert.NoError(t, p.Go(context.Background(), func(context.Context) {
			time.Sleep(cost)
			executed.Add(1)
		}))
	}

	// Close 开始后才放行，确保走的是关闭排空路径
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(block)
	}()

	start := time.Now()
	p.Close()
	elapsed := time.Since(start)

	assert.Equal(t, int64(cached), executed.Load(), "缓存中的任务被丢弃")

	serial := cached * cost
	assert.Less(t, elapsed, serial/3, "排空耗时接近串行执行(%v)，未并发", serial)
	t.Logf("排空 %d 个任务耗时 %v（串行需 %v）", cached, elapsed.Round(time.Millisecond), serial)
}

// 等待超时后 Shutdown 必须返回错误，让调用方知道有任务未跑完
func TestShutdownReportsTimeout(t *testing.T) {
	p := New(1)

	assert.NoError(t, p.Go(context.Background(), func(context.Context) {
		time.Sleep(time.Second)
	}))
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	assert.ErrorIs(t, p.Shutdown(ctx), context.DeadlineExceeded)

	// 超时只结束本次等待；再次调用可以等到任务执行结束。
	assert.NoError(t, p.Shutdown(context.Background()))
}

// 并发 Close 不能重复执行关闭流程（重复 close(chan) 会 panic）
func TestConcurrentCloseIsSafe(t *testing.T) {
	p := New(4, WithCacheSize(8))

	var executed atomic.Int64
	for range 8 {
		assert.NoError(t, p.Go(context.Background(), func(context.Context) { executed.Add(1) }))
	}

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Close()
		}()
	}

	waitGroup(t, &wg, 5*time.Second)
	assert.Equal(t, int64(8), executed.Load())
}
