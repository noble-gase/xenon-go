package worker

import "time"

// Option 协程池选项
type Option func(*pool)

// WithPrefill 预填充协程数量
func WithPrefill(n int) Option {
	return func(p *pool) {
		if n > 0 {
			p.prefill = n
		}
	}
}

// WithCacheSize 任务缓存容量，默认：0=不缓存
//
// 仅在协程数达到 capacity 之后才会用到：未达上限时新开协程直接执行，不经过缓存。
// 因此 capacity 设得过大可能会让本选项失效——
// New(10000, WithCacheSize(1000)) 要先有 10000 个任务同时在执行，缓存才会被写入。
//
// capacity 决定并发度（由下游承受力决定），本选项决定排队深度（由内存决定），
// 两者相互独立，需分别设置。缓存写满后 Go 重新变为阻塞
func WithCacheSize(n int) Option {
	return func(p *pool) {
		if n > 0 {
			p.cacheSize = n
		}
	}
}

// WithIdleTimeout 协程闲置超时时长，默认：10min
//
// 回收是按固定间隔轮询的（间隔为 max(1min, idleTimeout/10)，
// 避免过于频繁地争抢 LRU 锁影响任务执行），
// 因此实际回收时机落在 [idleTimeout, idleTimeout+间隔] 之间，不是精确值
func WithIdleTimeout(duration time.Duration) Option {
	return func(p *pool) {
		if duration > 0 {
			p.idleTimeout = duration
		}
	}
}

// WithPanicHandler 任务Panic处理方法
func WithPanicHandler(fn PanicFn) Option {
	return func(p *pool) {
		p.panicFn = fn
	}
}

// WithCloseTimeout Close 等待剩余任务执行完的时长，默认：30s。
// 超时后继续在后台排空；需要获取等待结果或再次等待请使用 Shutdown(ctx)
func WithCloseTimeout(duration time.Duration) Option {
	return func(p *pool) {
		if duration > 0 {
			p.closeTimeout = duration
		}
	}
}
