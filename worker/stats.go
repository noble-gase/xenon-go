package worker

// Stats 协程池运行时状态快照。
//
// 各字段是分别读取的，不是同一时刻的原子快照：适合监控面板和背压告警，
// 不要用来断言精确不变式（例如 Running+Pending 恒等于某值）
type Stats struct {
	Running   int // 正在执行任务的协程数
	Pending   int // 已接收、尚未开始执行的任务数
	Waiting   int // 停留在 Go 里的提交方数；持续 > 0 才代表遇到背压
	Live      int // 存活协程数（含空闲）
	Capacity  int // 并发上限
	CacheSize int // 配置的缓存容量
}

// Stats 读取当前运行时状态。无锁，字段逐个读取。
func (p *pool) Stats() Stats {
	return Stats{
		Running:   int(p.running.Load()),
		Pending:   int(p.pending.Load()),
		Waiting:   int(p.waiting.Load()),
		Live:      int(p.live.Load()),
		Capacity:  p.capacity,
		CacheSize: p.cacheSize,
	}
}
