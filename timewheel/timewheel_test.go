package timewheel

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/noble-gase/xenon/worker"
	"github.com/stretchr/testify/assert"
)

func TestTimeWheelRunsTaskOnceAndReleasesContext(t *testing.T) {
	for _, delay := range []time.Duration{0, 30 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			var calls, cancellations atomic.Int32
			tw := New(
				WithTimeLevel(Level(8, 10*time.Millisecond)),
				WithWorkerPool(worker.New(1)),
				WithCancelFn(func(context.Context, *Task) { cancellations.Add(1) }),
			)
			defer tw.Stop()
			parent := t.Context()
			execTime := time.Now().Add(delay)
			task := tw.Go(parent, "single", func(ctx context.Context, task *Task) {
				calls.Add(1)
				assert.Equal(t, "single", task.ID())
				assert.NoError(t, ctx.Err())
				assert.False(t, time.Now().Before(execTime), "task ran before its execution time")
			}, execTime)

			waitTaskDone(t, task)
			assert.Equal(t, int32(1), calls.Load())
			assert.ErrorIs(t, context.Cause(task.ctx), ErrTaskCompleted, "completion must be distinguishable from cancellation")
			assert.Zero(t, cancellations.Load(), "normal completion must not notify cancellation")
			assert.NoError(t, parent.Err(), "completion must not cancel the business parent")
		})
	}
}

func TestTaskCancelCallsCancelFn(t *testing.T) {
	canceled := make(chan string, 1)
	tw := New(
		WithTimeLevel(Level(8, 20*time.Millisecond)),
		WithWorkerPool(worker.New(1, worker.WithCacheSize(2))),
		WithCancelFn(func(ctx context.Context, task *Task) {
			canceled <- task.ID()
		}),
	)
	defer tw.Stop()

	task := tw.Go(context.Background(), "cancel-me", func(ctx context.Context, task *Task) {
		t.Error("canceled task should not run")
	}, time.Now().Add(10*time.Millisecond))
	task.Cancel(context.Canceled)

	select {
	case id := <-canceled:
		assert.Equal(t, "cancel-me", id)
	case <-time.After(time.Second):
		t.Fatal("cancel handler was not called")
	}
}

func TestContextDonePreventsTaskExecution(t *testing.T) {
	canceled := make(chan string, 1)
	tw := New(
		WithTimeLevel(Level(8, 20*time.Millisecond)),
		WithWorkerPool(worker.New(1, worker.WithCacheSize(2))),
		WithCancelFn(func(ctx context.Context, task *Task) {
			canceled <- task.ID()
		}),
	)
	defer tw.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	tw.Go(ctx, "ctx-cancel", func(ctx context.Context, task *Task) {
		t.Error("task with canceled context should not run")
	}, time.Now().Add(10*time.Millisecond))
	cancel()

	select {
	case id := <-canceled:
		assert.Equal(t, "ctx-cancel", id)
	case <-time.After(time.Second):
		t.Fatal("cancel handler was not called")
	}
}

func TestStopTerminatesPendingTask(t *testing.T) {
	notified := make(chan error, 1)
	tw := New(
		WithTimeLevel(Level(8, 20*time.Millisecond)),
		WithWorkerPool(worker.New(1, worker.WithCacheSize(2))),
		WithCancelFn(func(ctx context.Context, task *Task) {
			assert.Equal(t, "pending", task.ID())
			notified <- context.Cause(ctx)
		}),
	)

	var ran atomic.Int32
	parent := t.Context()
	task := tw.Go(parent, "pending", func(ctx context.Context, task *Task) {
		ran.Add(1)
	}, time.Now().Add(30*time.Millisecond))
	tw.Stop()

	// 残留任务必须释放 context 并通知，否则等待方永久阻塞
	waitTaskDone(t, task)
	assert.ErrorIs(t, context.Cause(task.ctx), ErrTimeWheelStopped)
	select {
	case cause := <-notified:
		assert.ErrorIs(t, cause, ErrTimeWheelStopped)
	case <-time.After(time.Second):
		t.Fatal("stop was not reported to the cancel handler")
	}

	time.Sleep(80 * time.Millisecond)
	assert.Equal(t, int32(0), ran.Load())
	assert.NoError(t, parent.Err(), "stop must not cancel the business parent")
}

func TestBucketResetReturnsPreviousTasks(t *testing.T) {
	bucket := NewBucket()
	task := &Task{id: "task-1"}
	assert.True(t, bucket.Add(task))

	old := bucket.Reset()
	assert.Equal(t, 1, old.Len())
	assert.Same(t, task, old.Front().Value)
	assert.Equal(t, 0, bucket.Reset().Len())
}

func TestBucketCloseRejectsFurtherTasks(t *testing.T) {
	bucket := NewBucket()
	task := &Task{id: "task-1"}
	assert.True(t, bucket.Add(task))

	old := bucket.Close()
	assert.Equal(t, 1, old.Len())
	assert.Same(t, task, old.Front().Value)
	assert.False(t, bucket.Add(&Task{id: "task-2"}), "closed bucket must reject tasks")
	assert.Equal(t, 0, bucket.Reset().Len())
}

func TestPoolSubmissionFailureCallsCancelFn(t *testing.T) {
	for _, delay := range []time.Duration{0, 30 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			p := worker.New(1)
			p.Close()
			notified := make(chan error, 1)
			tw := New(
				WithTimeLevel(Level(8, 10*time.Millisecond)),
				WithWorkerPool(p),
				WithCancelFn(func(ctx context.Context, task *Task) {
					assert.Equal(t, "rejected", task.ID())
					notified <- context.Cause(ctx)
				}),
			)
			defer tw.Stop()
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			task := tw.Go(parent, "rejected", func(context.Context, *Task) {
				t.Error("rejected task should not run")
			}, time.Now().Add(delay))

			select {
			case cause := <-notified:
				assert.ErrorIs(t, cause, worker.ErrPoolClosed)
			case <-time.After(time.Second):
				t.Fatal("submission failure was not reported")
			}
			assert.ErrorIs(t, context.Cause(task.ctx), worker.ErrPoolClosed)
			assert.NoError(t, parent.Err())
		})
	}
}

func TestBlockedSubmissionUsesBusinessDeadline(t *testing.T) {
	p := worker.New(1)
	notified := make(chan error, 1)
	tw := New(
		WithWorkerPool(p),
		WithCancelFn(func(ctx context.Context, _ *Task) { notified <- context.Cause(ctx) }),
	)
	defer tw.Stop()
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	if err := p.Go(context.Background(), func(context.Context) {
		close(started)
		<-release
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	// 占满唯一工作协程，并让池调度器持有另一个待派发任务。
	if err := p.Go(context.Background(), func(context.Context) {}); err != nil {
		t.Fatal(err)
	}
	want := errors.New("business submission deadline")
	ctx, cancel := context.WithTimeoutCause(context.Background(), 30*time.Millisecond, want)
	defer cancel()
	task := tw.Go(ctx, "blocked", func(context.Context, *Task) {
		t.Error("submission that timed out should not run")
	}, time.Now())

	assert.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	assert.ErrorIs(t, context.Cause(task.ctx), want)
	select {
	case cause := <-notified:
		assert.ErrorIs(t, cause, want)
	case <-time.After(time.Second):
		t.Fatal("submission timeout was not reported")
	}
}

func TestBusinessCanScheduleNextTaskWithParentContext(t *testing.T) {
	tw := New(
		WithTimeLevel(Level(8, 10*time.Millisecond)),
		WithWorkerPool(worker.New(1)),
	)
	defer tw.Stop()
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	nextTasks := make(chan *Task, 1)
	first := tw.Go(parent, "first", func(context.Context, *Task) {
		calls.Add(1)
		// 使用业务父 context，不继承本次回调结束时的取消信号。
		nextTasks <- tw.Go(parent, "next", func(context.Context, *Task) {
			calls.Add(1)
		}, time.Now().Add(30*time.Millisecond))
	}, time.Now())
	waitTaskDone(t, first)
	select {
	case next := <-nextTasks:
		waitTaskDone(t, next)
	case <-time.After(time.Second):
		t.Fatal("next task was not submitted")
	}
	assert.Equal(t, int32(2), calls.Load())
	assert.NoError(t, parent.Err())
}

func TestTaskPanicReleasesContext(t *testing.T) {
	recovered := make(chan any, 1)
	tw := New(WithWorkerPool(worker.New(1, worker.WithPanicHandler(func(_ context.Context, err any, _ []byte) {
		recovered <- err
	}))))
	defer tw.Stop()
	task := tw.Go(context.Background(), "panic", func(context.Context, *Task) {
		panic("boom")
	}, time.Now())
	select {
	case err := <-recovered:
		assert.Equal(t, "boom", err)
	case <-time.After(time.Second):
		t.Fatal("task panic was not recovered")
	}
	waitTaskDone(t, task)
	assert.ErrorIs(t, context.Cause(task.ctx), context.Canceled)
	assert.NotErrorIs(t, context.Cause(task.ctx), ErrTaskCompleted, "panic must not be reported as completion")
}

func TestStopDuringExpirationWaitPreventsExecution(t *testing.T) {
	for _, busy := range []bool{false, true} {
		name := "worker_waiting_for_expiration"
		if busy {
			name = "dispatcher_holding_task"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := worker.New(1)
				var ran, notified atomic.Int32
				tw := New(
					WithTimeLevel(Level(8, 10*time.Second)),
					WithWorkerPool(p),
					WithCancelFn(func(ctx context.Context, _ *Task) {
						assert.ErrorIs(t, context.Cause(ctx), ErrTimeWheelStopped)
						notified.Add(1)
					}),
				)
				defer tw.Stop()
				if busy {
					release := make(chan struct{})
					defer close(release)
					assert.NoError(t, p.Go(context.Background(), func(context.Context) {
						select {
						case <-release:
						case <-tw.(*timewheel).ctx.Done():
						}
					}))
					synctest.Wait()
				}
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				task := tw.Go(parent, "future", func(context.Context, *Task) {
					ran.Add(1)
				}, time.Now().Add(5*time.Second))
				synctest.Wait()

				stopped := make(chan struct{})
				go func() {
					tw.Stop()
					close(stopped)
				}()
				synctest.Wait()
				// 到期等待可被中断：Stop 无需等到原执行时间
				select {
				case <-stopped:
				default:
					t.Fatal("Stop blocked on the expiration wait")
				}
				time.Sleep(5 * time.Second)
				<-stopped
				synctest.Wait()
				assert.ErrorIs(t, context.Cause(task.ctx), ErrTimeWheelStopped)
				assert.Equal(t, int32(1), notified.Load())
				assert.Zero(t, ran.Load())
				assert.NoError(t, parent.Err())
			})
		})
	}
}

func TestTaskCancelDuringExpirationWaitPreventsExecution(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		want := errors.New("business canceled task")
		var ran, notified atomic.Int32
		tw := New(
			WithTimeLevel(Level(8, 10*time.Second)),
			WithWorkerPool(worker.New(1)),
			WithCancelFn(func(ctx context.Context, _ *Task) {
				assert.ErrorIs(t, context.Cause(ctx), want)
				notified.Add(1)
			}),
		)
		defer tw.Stop()
		task := tw.Go(context.Background(), "future", func(context.Context, *Task) {
			ran.Add(1)
		}, time.Now().Add(5*time.Second))
		synctest.Wait()
		task.Cancel(want)
		synctest.Wait()
		assert.ErrorIs(t, context.Cause(task.ctx), want)
		time.Sleep(5 * time.Second)
		synctest.Wait()
		assert.Equal(t, int32(1), notified.Load())
		assert.Zero(t, ran.Load())
	})
}

func TestStopReportsCauseForBlockedSubmission(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "wheel_stopped"
		if canceled {
			name = "business_cause_preserved"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := worker.New(1)
				want := ErrTimeWheelStopped
				var notified atomic.Int32
				tw := New(
					WithWorkerPool(p),
					WithCancelFn(func(ctx context.Context, _ *Task) {
						assert.ErrorIs(t, context.Cause(ctx), want)
						notified.Add(1)
					}),
				)
				defer tw.Stop()
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				assert.NoError(t, p.Go(context.Background(), func(context.Context) { <-release }))
				synctest.Wait()
				// 池调度器持有第二个任务；下一次提交会等待 input 接收方。
				assert.NoError(t, p.Go(context.Background(), func(context.Context) {}))
				synctest.Wait()
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				returned := make(chan *Task, 1)
				go func() {
					defer unblock()
					returned <- tw.Go(ctx, "blocked", func(context.Context, *Task) {
						t.Error("rejected task must not run")
					}, time.Now())
				}()
				synctest.Wait()
				assert.Empty(t, returned, "submission must still be blocked")
				if canceled {
					want = errors.New("business cancellation before Stop")
					cancel(want)
				}
				tw.Stop()
				synctest.Wait()
				select {
				case task := <-returned:
					assert.ErrorIs(t, context.Cause(task.ctx), want)
				default:
					t.Fatal("Stop did not unblock submission")
				}
				assert.Equal(t, int32(1), notified.Load())
			})
		})
	}
}

func waitTaskDone(t *testing.T, task *Task) {
	t.Helper()

	select {
	case <-task.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("task context was not released")
	}
}
