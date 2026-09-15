package timewheel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/noble-gase/xenon/worker"
)

var (
	// ErrTaskCompleted 任务正常执行完毕后释放 context 的原因
	ErrTaskCompleted = errors.New("timewheel: task completed")

	// ErrTimeWheelStopped 任务因时间轮停止而终止的原因
	ErrTimeWheelStopped = errors.New("timewheel: stopped")
)

type (
	// TaskFn 单次任务方法，返回后释放本次任务的 context，不自动重试。
	// 若需提交后续任务，应使用业务父 context，避免随本次任务结束而取消。
	TaskFn func(ctx context.Context, task *Task)

	// CancelFn 任务 context「超时｜取消」、入池失败或时间轮停止的处理方法。
	// 失败原因通过 context.Cause(ctx) 获取；正常完成后的 context 释放不触发此回调。
	CancelFn func(ctx context.Context, task *Task)
)

// TimeWheel 时间轮
type TimeWheel interface {
	// Go 提交单次任务到时间轮
	//
	// 任务在协程池中异步执行，若 context 超时｜取消，则任务也随之取消；
	// 但延迟小于最小精度时，提交动作本身会同步等待协程池容量，等待是否超时由 context 决定。
	//
	// 注意：在本时间轮的任务回调中再次调用 Go 时，应避免所有工作协程无期限等待入池。
	// 若需脱离调用方取消信号，可由业务显式使用 context.WithoutCancel(ctx)。
	Go(ctx context.Context, taskId string, taskFn TaskFn, execTime time.Time) *Task

	// Stop 终止时间轮，残留任务的 context 以 ErrTimeWheelStopped 释放并触发 CancelFn
	Stop()
}

// TimeLevel 层级
type TimeLevel struct {
	size int

	prec  time.Duration
	round time.Duration // 一圈时长

	slot atomic.Int32 // 当前槽位

	buckets []*Bucket
}

func (tl *TimeLevel) String() string {
	return fmt.Sprintf("size=%d, prec=%s, slot=%d", tl.size, tl.prec.String(), tl.slot.Load())
}

// Level 返回一个层级
func Level(size int, prec time.Duration) *TimeLevel {
	return &TimeLevel{
		size:    size,
		prec:    prec,
		round:   prec * time.Duration(size),
		buckets: make([]*Bucket, size),
	}
}

func Day(n int) *TimeLevel {
	return Level(n, time.Hour*24)
}

func Hour() *TimeLevel {
	return Level(24, time.Hour)
}

func Minute() *TimeLevel {
	return Level(60, time.Minute)
}

func Second() *TimeLevel {
	return Level(60, time.Second)
}

type timewheel struct {
	levels []*TimeLevel
	prec   time.Duration // 最小精度

	pool worker.Pool

	cancelFn CancelFn

	ctx    context.Context
	cancel context.CancelFunc
}

func (tw *timewheel) Go(ctx context.Context, taskId string, taskFn TaskFn, execTime time.Time) *Task {
	ctx, cancel := context.WithCancelCause(ctx)

	task := &Task{
		id: taskId,

		execFunc: taskFn,
		execTime: execTime,

		ctx:    ctx,
		cancel: cancel,
	}

	// 入时间轮
	tw.requeue(task)

	return task
}

func (tw *timewheel) Stop() {
	select {
	case <-tw.ctx.Done(): // 时间轮已停止
		return
	default:
	}

	tw.cancel()
	tw.pool.Close()

	// 终止残留任务，释放 context 并通知，避免调用方永久等待
	for _, tl := range tw.levels {
		for _, b := range tl.buckets {
			taskList := b.Close()
			if taskList == nil {
				continue
			}
			for e := taskList.Front(); e != nil; e = e.Next() {
				tw.abort(e.Value.(*Task), ErrTimeWheelStopped)
			}
		}
	}
}

// abort 终止任务：释放 context 并通知取消回调
//
// context 已释放时保留原有原因，cause 仅在首次释放时生效
func (tw *timewheel) abort(task *Task, cause error) {
	task.cancel(cause)

	if tw.cancelFn != nil {
		// 防止 cancelFn 打穿工作协程并终止整个进程
		defer func() { _ = recover() }()

		tw.cancelFn(task.ctx, task)
	}
}

func (tw *timewheel) requeue(task *Task) {
	select {
	case <-tw.ctx.Done(): // 时间轮已停止
		tw.abort(task, ErrTimeWheelStopped)
		return
	case <-task.ctx.Done(): // 任务被取消
		tw.abort(task, context.Canceled)
		return
	default:
	}

	delay := time.Until(task.execTime)
	if delay < tw.prec {
		tw.do(task)
		return
	}

	var tl *TimeLevel
	for _, tl = range tw.levels {
		if delay > tl.prec {
			break
		}
	}

	var mod int
	if tl.prec > tw.prec {
		mod = int(delay / tl.prec)
	} else {
		mod = int((delay + tl.prec - 1) / tl.prec)
	}
	slot := (mod%tl.size + int(tl.slot.Load())) % tl.size

	// 任务入槽位
	if !tl.buckets[slot].Add(task) { // 时间轮已停止
		tw.abort(task, ErrTimeWheelStopped)
		return
	}

	slog.LogAttrs(
		task.ctx, slog.LevelInfo, "[timewheel] task requeue",
		slog.String("task_id", task.ID()),
		slog.Int64("exec_time", task.execTime.UnixNano()),
		slog.Int64("delay", delay.Nanoseconds()),
		slog.Int("mod", mod),
		slog.Int("slot", slot),
		slog.String("level", tl.String()),
	)
}

func (tw *timewheel) scheduler() {
	for _, v := range tw.levels {
		go func(tl *TimeLevel) {
			ticker := time.NewTicker(tl.prec)
			defer ticker.Stop()

			for {
				select {
				case <-tw.ctx.Done(): // 时间轮已停止
					return
				case <-ticker.C:
					slot := (int(tl.slot.Load()) + 1) % tl.size
					tl.slot.Store(int32(slot))
					tw.process(tl, slot)
				}
			}
		}(v)
	}
}

func (tw *timewheel) process(tl *TimeLevel, slot int) {
	taskList := tl.buckets[slot].Reset()

	go func() {
		// 同一槽位串行提交；协程池满时，后续任务的派发也会等待。
		for e := taskList.Front(); e != nil; e = e.Next() {
			task := e.Value.(*Task)

			delay := time.Until(task.execTime)
			if delay < tw.prec {
				tw.do(task)
				continue
			}
			// 重新入时间轮
			if delay < tl.round {
				tw.requeue(task)
				continue
			}
			// 放回原槽位，等待下一轮
			if !tl.buckets[slot].Add(task) { // 时间轮已停止
				tw.abort(task, ErrTimeWheelStopped)
			}
		}
	}()
}

func (tw *timewheel) do(task *Task) {
	select {
	case <-tw.ctx.Done(): // 时间轮停止
		tw.abort(task, ErrTimeWheelStopped)
		return
	case <-task.ctx.Done(): // 任务被取消
		tw.abort(task, context.Canceled)
		return
	default:
	}

	// 先入池后等待 vs time.AfterFunc：
	//  - 先入池后等待：排队耗时发生在等待前且被重新计算的剩余延迟抵消，精度高；
	//    等待期间占用池协程，但不超过一个最小精度周期，影响可忽略；
	//  - AfterFunc：先等待后入池，等待期间不占协程，但到期后才入池，排队耗时直接成为迟到误差（批量到期时可达秒级）
	err := tw.pool.Go(task.ctx, func(ctx context.Context) {
		// 异常退出时兜底释放 context；panic 仍由协程池处理。
		defer task.cancel(context.Canceled)

		// 等待到期，期间响应「任务取消」和「时间轮停止」
		if cause := tw.wait(ctx, task.execTime); cause != nil {
			tw.abort(task, cause)
			return
		}

		task.execFunc(ctx, task)
		task.cancel(ErrTaskCompleted)
	})
	if err != nil {
		if errors.Is(err, worker.ErrPoolClosed) && tw.ctx.Err() != nil {
			err = ErrTimeWheelStopped
		}
		tw.abort(task, err)
	}
}

// wait 等待任务到期，返回非 nil 表示需要终止任务的原因
func (tw *timewheel) wait(ctx context.Context, execTime time.Time) error {
	timer := time.NewTimer(time.Until(execTime))
	defer timer.Stop()

	select {
	case <-tw.ctx.Done(): // 时间轮停止
		return ErrTimeWheelStopped
	case <-ctx.Done(): // 任务被取消
		return context.Canceled
	case <-timer.C:
	}

	// 到期与终止信号同时就绪时 select 随机，复查一次保证优先终止
	select {
	case <-tw.ctx.Done(): // 时间轮停止
		return ErrTimeWheelStopped
	case <-ctx.Done(): // 任务被取消
		return context.Canceled
	default:
		return nil
	}
}

// New 返回一个时间轮
func New(opts ...Option) TimeWheel {
	ctx, cancel := context.WithCancel(context.Background())

	tw := &timewheel{
		ctx:    ctx,
		cancel: cancel,
	}
	for _, fn := range opts {
		fn(tw)
	}

	if tw.pool == nil {
		tw.pool = worker.New(10000, worker.WithCacheSize(1000))
	}

	if tw.cancelFn == nil {
		tw.cancelFn = func(ctx context.Context, task *Task) {
			slog.LogAttrs(
				ctx, slog.LevelWarn, "[timewheel] task canceled",
				slog.String("task_id", task.ID()),
				slog.Any("reason", context.Cause(ctx)),
			)
		}
	}

	// 层级
	if len(tw.levels) == 0 {
		tw.levels = []*TimeLevel{Hour(), Minute(), Second()}
	}
	sort.SliceStable(tw.levels, func(i, j int) bool {
		if tw.levels[i].prec == tw.levels[j].prec {
			return tw.levels[i].size > tw.levels[j].size
		}
		return tw.levels[i].prec > tw.levels[j].prec
	})

	// 初始化槽位
	for _, v := range tw.levels {
		if tw.prec == 0 || tw.prec > v.prec {
			tw.prec = v.prec
		}

		for i := range v.size {
			v.buckets[i] = NewBucket()
		}
	}

	go tw.scheduler()

	return tw
}

var (
	tw   TimeWheel
	once sync.Once
)

// Init 初始化默认的全局时间轮
func Init(opts ...Option) {
	tw = New(opts...)
}

// Go 使用默认的全局时间轮
func Go(ctx context.Context, taskId string, taskFn TaskFn, execTime time.Time) *Task {
	if tw == nil {
		once.Do(func() {
			tw = New()
		})
	}
	return tw.Go(ctx, taskId, taskFn, execTime)
}

// Close 关闭默认的全局时间轮
func Stop() {
	if tw != nil {
		tw.Stop()
	}
}
