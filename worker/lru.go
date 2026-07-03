package worker

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type worker struct {
	id        int64
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

	lru.wkMap[w.id] = lru.wkList.PushFront(w)
}

func (lru *WorkerLRU) IdleCheck(timeout time.Duration) {
	lru.mutex.Lock()
	defer lru.mutex.Unlock()

	now := time.Now().UnixNano()

	for e := lru.wkList.Back(); e != nil; {
		w := e.Value.(*worker)

		// 任务执行中，跳过
		if w.busy.Load() {
			e = e.Prev()
			continue
		}

		// 未超时，直接结束
		if now-w.keepalive.Load() < timeout.Nanoseconds() {
			break
		}

		prev := e.Prev()

		// 超时，移除worker
		lru.wkList.Remove(e)
		delete(lru.wkMap, w.id)
		// 取消，关闭协程
		w.cancel()

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
