package timewheel

import "github.com/noble-gase/xenon/worker"

// Option 时间轮选项
type Option func(tw *timewheel)

// WithCancelFn 指定任务 context「超时｜取消」、入池失败或时间轮停止的处理方法。
// 失败原因通过 context.Cause(ctx) 获取；正常完成后的 context 释放不触发此回调。
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
