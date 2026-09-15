package timewheel

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// Task 任务
type Task struct {
	id string // 任务ID

	execFunc TaskFn    // 任务执行函数
	execTime time.Time // 任务执行时间

	ctx    context.Context
	cancel context.CancelCauseFunc
}

// ID 返回任务ID
func (t *Task) ID() string {
	return t.id
}

// Cancel 取消任务
func (t *Task) Cancel(err error) {
	t.cancel(err)
}

type Bucket struct {
	list   *list.List
	closed bool

	mutex sync.Mutex
}

// Add 任务入槽位，槽位已关闭时返回 false
func (b *Bucket) Add(t *Task) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.closed {
		return false
	}

	if b.list == nil {
		b.list = list.New()
	}
	b.list.PushBack(t)

	return true
}

// Reset 取出并清空槽位内的任务
func (b *Bucket) Reset() *list.List {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	old := b.list
	b.list = list.New()
	return old
}

// Close 关闭槽位并取出残留任务，后续 Add 均失败
func (b *Bucket) Close() *list.List {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.closed = true

	old := b.list
	b.list = list.New()
	return old
}

func NewBucket() *Bucket {
	return &Bucket{
		list: list.New(),
	}
}
