package worker

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type worker struct {
	id int64

	busy      atomic.Bool
	keepalive atomic.Int64

	ctx    context.Context
	cancel context.CancelFunc
}

type WorkerLRU struct {
	wkMap  map[int64]*list.Element
	wkList *list.List

	mutex sync.Mutex
}

func (lru *WorkerLRU) Upsert(w *worker) {
	lru.mutex.Lock()
	defer lru.mutex.Unlock()

	select {
	case <-w.ctx.Done(): // 已取消
		return
	default:
	}

	w.keepalive.Store(time.Now().UnixNano())

	// 存在，移到头部
	if e, ok := lru.wkMap[w.id]; ok {
		lru.wkList.MoveToFront(e)
		return
	}
	// 不存在，插入头部
	lru.wkMap[w.id] = lru.wkList.PushFront(w)
}

// Remove 移除指定协程，幂等。协程退出时自行调用，
// 确保异常退出（panic 等）的协程也能从 LRU 中摘除
func (lru *WorkerLRU) Remove(w *worker) {
	lru.mutex.Lock()
	defer lru.mutex.Unlock()

	e, ok := lru.wkMap[w.id]
	if !ok {
		return
	}

	lru.wkList.Remove(e)
	delete(lru.wkMap, w.id)
}

func (lru *WorkerLRU) IdleCheck(timeout time.Duration) {
	lru.mutex.Lock()
	defer lru.mutex.Unlock()

	now := time.Now().UnixNano()

	for e := lru.wkList.Back(); e != nil; {
		w := e.Value.(*worker)

		// 未超时，直接结束
		if now-w.keepalive.Load() < timeout.Nanoseconds() {
			break
		}

		prev := e.Prev()
		// 超时，且无任务执行，移除。
		// 两步在同一次持锁内完成，Upsert 进不来，顺序本无差别；
		// 仍与 retire 统一为 cancel-first，防止将来拆细锁或插入提前返回时漏掉那个窗口
		if !w.busy.Load() {
			// 取消，关闭协程
			w.cancel()

			lru.wkList.Remove(e)
			delete(lru.wkMap, w.id)
		}
		e = prev
	}
}

func (lru *WorkerLRU) Len() int {
	lru.mutex.Lock()
	defer lru.mutex.Unlock()

	return lru.wkList.Len()
}

func NewWorkerLRU() *WorkerLRU {
	return &WorkerLRU{
		wkMap:  make(map[int64]*list.Element),
		wkList: list.New(),
	}
}
