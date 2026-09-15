package worker

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

var ErrPoolClosed = errors.New("pool closed")

// cacheLine 用于伪共享填充的缓存行宽度，取各平台最大值：
// arm64 / darwin-arm64 是 128 字节，x86-64 是 64。
// 按最大值填充，少则不起作用，多则只多占几十字节
const cacheLine = 128

const (
	defaultCap   = 10000
	defaultCache = 1000
)

// Pool 协程并发复用，降低「CPU」和「内存」负载
type Pool interface {
	// Go 执行任务，没有闲置协程时入缓存队列，缓存达到上限会阻塞等待
	//
	// 通常需要 context.WithoutCancel(ctx)
	//
	// fn 不能包含无界的阻塞：fn 不返回，它占用的并发名额就不会释放，
	// 占满 capacity 后新任务再也投递不出去，Close / Shutdown 也只能空等到超时。
	// 有上界的等待（IO、加锁）是正常的，必须避免的是：
	//   - 等待同一个池里的其它任务：池已满时子任务提交不进去，必然自锁
	//   - 在 fn 里关闭本池：关闭要等 fn 自己先退出，等不到
	//   - 等待没有超时保证的信号：无 deadline 的 chan 接收、无超时的网络读
	//
	// 注意池关闭不会取消 fn 收到的 ctx（那就是提交时传入的 ctx），
	// 需要 fn 可被打断的话，得由提交方给一个带 deadline 的 ctx，并在 fn 内自行响应
	Go(ctx context.Context, fn func(ctx context.Context)) error

	// Close 优雅关闭：停止接收新任务，排空缓存并等待执行中的任务结束。
	// 等待时长由 WithCloseTimeout 控制（默认 30s），超时后在后台继续排空
	Close()

	// Shutdown 与 Close 相同，但由 ctx 控制等待时长。
	//
	// ctx 结束时仍有任务未执行完则返回 context.Cause(ctx)，后台继续排空。
	// 每次调用独立等待；超时后可以再次调用，等待剩余任务执行结束
	Shutdown(ctx context.Context) error

	// Stats 读取运行时状态，用于监控和背压告警。
	//
	// 各字段分别读取，不是原子快照，不要用来断言精确不变式
	Stats() Stats
}

// PanicFn 处理Panic方法
type PanicFn func(ctx context.Context, err any, stack []byte)

type task struct {
	ctx context.Context
	fn  func(ctx context.Context)
}

type pool struct {
	input chan *task
	queue chan *task

	cache     chan *task
	cacheSize int

	capacity int
	prefill  int

	uniqID  atomic.Int64
	workers *WorkerLRU

	// live 在协程实际退出时递减，LRU 提前标记回收不会释放并发名额。
	live atomic.Int64

	// 以下三个计数仅供 Stats 读取，不参与任何调度判断。
	running atomic.Int64 // 正在执行任务的协程数
	pending atomic.Int64 // 已接收、尚未开始执行的任务数

	// waiting 由所有提交方并发写，独占缓存行：
	// 与 live / pending 同行会造成伪共享——每次提交都作废调度协程手里的 live，
	// 而 deliver 的热路径每轮都要读 live。实测混在一起会慢 5%。
	//
	// cacheLine 取各平台最大值（arm64 是 128，x86 是 64），不是按本机实测行宽，
	// 否则换架构编译出来的填充就不够宽了
	_       [cacheLine]byte
	waiting atomic.Int64 // 停留在 Go 里的提交方数
	_       [cacheLine]byte

	idleTimeout time.Duration

	panicFn PanicFn

	// 只有初始化和 run 能创建 worker；run 投递完全部任务后才调用 Wait。
	wg sync.WaitGroup

	drain chan struct{} // 全部任务已交给 worker，执行结束后退出

	closeTimeout time.Duration

	refill chan struct{} // 协程退出信号，唤醒 run 补充协程
	done   chan struct{} // 全部任务和 worker 已结束

	ctx    context.Context
	cancel context.CancelFunc
}

// Go 提交任务，投递成功即保证执行；对 fn 的约束见 Pool.Go
func (p *pool) Go(ctx context.Context, fn func(ctx context.Context)) error {
	p.waiting.Add(1)
	defer p.waiting.Add(-1)

	// 契约是「接收即执行，拒绝即未执行」，不是「关闭后必定拒绝」：
	// 与 cancel 并发的提交仍可能被接收，由 run 的关闭序列保证它执行完毕
	select {
	case <-p.ctx.Done(): // Pool关闭
		return ErrPoolClosed
	case <-ctx.Done():
		return context.Cause(ctx)
	case p.input <- &task{ctx: ctx, fn: fn}:
		return nil
	}
}

func (p *pool) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), p.closeTimeout)
	defer cancel()

	_ = p.Shutdown(ctx)
}

func (p *pool) Shutdown(ctx context.Context) error {
	// cancel 是幂等的；run 在后台完成投递和排空，各调用方独立等待。
	p.cancel()

	// 已完成的关闭不受本次 ctx 状态影响。必须单独判定：done 与 ctx 同时就绪时，
	// 下面的 select 会随机选中，已经关完的池将有一半概率返回错误
	select {
	case <-p.done:
		return nil
	default:
	}

	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (p *pool) run() {
	defer close(p.done)

	for {
		select {
		case <-p.ctx.Done():
			p.drainCache()
			close(p.drain)
			p.wg.Wait()
			return
		case <-p.refill:
			// 没有新提交时也要恢复缓存任务的执行，不能只在 deliver 中补充。
			for n := len(p.cache); n > 0 && p.live.Load() < int64(p.capacity); n-- {
				p.spawn()
			}
		case v := <-p.input:
			// 接收即承诺执行，从这里开始计入 Pending，直到 exec 取走
			p.pending.Add(1)

			select {
			case p.queue <- v:
			default:
				p.deliver(v, p.cache)
			}
		}
	}
}

// deliver 把已接收的任务交给 worker 或缓存；关闭不会丢弃持有的任务。
// 排空时 cache 传 nil，防止任务被重新放回缓存。
func (p *pool) deliver(v *task, cache chan *task) {
	for {
		// 未达上限，新开一个协程
		if p.live.Load() < int64(p.capacity) {
			p.spawn()
		}

		// 等待闲置协程
		select {
		case p.queue <- v:
			return
		case cache <- v:
			return
		case <-p.refill:
			// worker 退出释放了名额，重新检查容量后投递。
		}
	}
}

// drainCache 由 run 独占写入端；缓存为空且手上的任务已交给 worker 后才能结束。
func (p *pool) drainCache() {
	for {
		select {
		case v := <-p.cache:
			p.deliver(v, nil)
		default:
			return
		}
	}
}

func (p *pool) idle() {
	ticker := time.NewTicker(max(time.Minute, p.idleTimeout/10))
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done(): // Pool关闭
			return
		case <-ticker.C:
			p.workers.IdleCheck(p.idleTimeout)
		}
	}
}

func (p *pool) spawn() {
	ctx, cancel := context.WithCancel(context.Background())

	wk := &worker{
		id: p.uniqID.Add(1),

		ctx:    ctx,
		cancel: cancel,
	}

	p.live.Add(1)
	p.wg.Add(1)
	p.workers.Upsert(wk)

	go func() {
		// 无论因何退出（闲置回收 / Pool关闭 / 异常），都要归还计数
		defer p.retire(wk)

		for {
			// 获取任务
			select {
			case <-p.drain: // 全部任务已分发，当前任务已执行结束
				return
			case <-wk.ctx.Done(): // 闲置超时，销毁
				return
			case v := <-p.queue: // 从队列获取任务
				p.exec(wk, v)
			case v := <-p.cache: // 从缓存获取任务
				p.exec(wk, v)
			}
		}
	}()
}

// retire 协程退出时摘除自身、归还计数，并唤醒 run 检查积压任务。
func (p *pool) retire(wk *worker) {
	defer p.wg.Done()

	// 先 cancel 再摘除，避免此刻的 Upsert 又把自己加回去
	wk.cancel()
	p.workers.Remove(wk)

	p.live.Add(-1)

	// 信号可合并；run 收到后总会重新检查存活数和缓存。
	select {
	case p.refill <- struct{}{}:
	default:
	}
}

func (p *pool) exec(wk *worker, v *task) {
	p.pending.Add(-1)
	p.running.Add(1)
	// 必须用 defer：任务里的 runtime.Goexit 会跳过后续语句但仍执行 defer，
	// 否则计数只增不减。下面的 busy 不需要，retire 会把 worker 从 LRU 摘掉兜底
	defer p.running.Add(-1)

	// 标记执行中，防止被 IdleCheck 误杀
	wk.busy.Store(true)
	p.workers.Upsert(wk)

	p.do(v)

	// 刷新 keepalive 后再解除标记，避免竞态
	p.workers.Upsert(wk)
	wk.busy.Store(false)
}

func (p *pool) do(task *task) {
	if task == nil || task.fn == nil {
		return
	}

	defer func() {
		r := recover()
		if r == nil || p.panicFn == nil {
			return
		}

		// panicFn 是在 recover 之后执行的，它自己再 panic 就没人接了，
		// 会打穿工作协程并终止整个进程，所以单独再兜一层
		defer func() { _ = recover() }()

		p.panicFn(task.ctx, r, debug.Stack())
	}()

	task.fn(task.ctx)
}

// New 生成一个新的Pool
func New(cap int, opts ...Option) Pool {
	if cap <= 0 {
		cap = defaultCap
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &pool{
		input: make(chan *task),

		capacity: cap,

		workers: NewWorkerLRU(),

		idleTimeout: 10 * time.Minute,

		drain: make(chan struct{}),

		closeTimeout: 30 * time.Second,

		refill: make(chan struct{}, 1),
		done:   make(chan struct{}),

		ctx:    ctx,
		cancel: cancel,
	}

	for _, fn := range opts {
		fn(p)
	}
	p.queue = make(chan *task)

	// 不缓存时保持 cache 为 nil：nil channel 的 select 分支永不就绪，
	// 等价于「只走 queue」。建成无缓冲 channel 则是 queue 的等价复制，白付一次 select 开销
	if p.cacheSize > 0 {
		p.cache = make(chan *task, p.cacheSize)
	}

	// 预填充
	if p.prefill > 0 {
		count := min(p.prefill, p.capacity)
		for range count {
			p.spawn()
		}
	}

	if p.panicFn == nil {
		p.panicFn = func(ctx context.Context, err any, stack []byte) {
			slog.LogAttrs(ctx, slog.LevelError, "panic recoverd", slog.Any("error", err), slog.String("stack", string(debug.Stack())))
		}
	}

	go p.run()
	go p.idle()

	return p
}

var (
	pp   Pool
	once sync.Once
)

// Init 初始化默认的全局Pool
func Init(cap int, opts ...Option) {
	pp = New(cap, opts...)
}

// Go 使用默认的全局Pool
func Go(ctx context.Context, fn func(ctx context.Context)) error {
	if pp == nil {
		once.Do(func() {
			pp = New(defaultCap, WithCacheSize(defaultCache))
		})
	}
	return pp.Go(ctx, fn)
}

// Close 关闭默认的全局Pool
func Close() {
	if pp != nil {
		pp.Close()
	}
}

// Shutdown 关闭默认的全局Pool，由 ctx 控制等待时长
func Shutdown(ctx context.Context) error {
	if pp == nil {
		return nil
	}
	return pp.Shutdown(ctx)
}
