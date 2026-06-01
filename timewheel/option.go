package timewheel

import "github.com/noble-gase/xenon/worker"

// Option 时间轮选项
type Option func(tw *timewheel)

// WithCancelFn 指定任务 context「超时｜取消」的处理方法
func WithCancelFn(fn CancelFn) Option {
	return func(tw *timewheel) {
		tw.cancelFn = fn
	}
}

// WithWorkerPool 指定协程池；默认：worker.New(10000, worker.WithCacheSize(1000))
func WithWorkerPool(pool worker.Pool) Option {
	return func(tw *timewheel) {
		tw.pool = pool
	}
}

// WithTimeLevel 指定时间轮层级；默认：3层，精度分别为：时、分、秒
func WithTimeLevel(levels ...*TimeLevel) Option {
	return func(tw *timewheel) {
		tw.levels = append(tw.levels, levels...)
	}
}
