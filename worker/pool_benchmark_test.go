package worker

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	RunTimes   = 1e6
	PoolCap    = 5e4
	BenchParam = 10
)

func demoFunc(ctx context.Context) {
	time.Sleep(time.Duration(BenchParam) * time.Millisecond)
}

func BenchmarkGoroutines(b *testing.B) {
	runtime.GOMAXPROCS(1)
	ctx := context.TODO()

	var wg sync.WaitGroup
	for b.Loop() {
		wg.Add(RunTimes)
		for range int(RunTimes) {
			go func() {
				demoFunc(ctx)
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

func BenchmarkChannel(b *testing.B) {
	runtime.GOMAXPROCS(1)
	ctx := context.TODO()

	var wg sync.WaitGroup
	sema := make(chan struct{}, PoolCap)

	for b.Loop() {
		wg.Add(RunTimes)
		for range int(RunTimes) {
			sema <- struct{}{}
			go func() {
				demoFunc(ctx)
				<-sema
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

func BenchmarkErrGroup(b *testing.B) {
	runtime.GOMAXPROCS(1)
	ctx := context.TODO()

	var wg sync.WaitGroup
	var eg errgroup.Group
	eg.SetLimit(PoolCap)

	for b.Loop() {
		wg.Add(RunTimes)
		for range int(RunTimes) {
			eg.Go(func() error {
				demoFunc(ctx)
				wg.Done()
				return nil
			})
		}
		wg.Wait()
	}
}

func BenchmarkWorkerPool(b *testing.B) {
	runtime.GOMAXPROCS(1)
	ctx := context.TODO()

	p := New(PoolCap)
	defer p.Close()

	var wg sync.WaitGroup
	for b.Loop() {
		wg.Add(RunTimes)
		for range int(RunTimes) {
			_ = p.Go(ctx, func(ctx context.Context) {
				demoFunc(ctx)
				wg.Done()
			})
		}
		wg.Wait()
	}
}

func BenchmarkGoroutinesThroughput(b *testing.B) {
	runtime.GOMAXPROCS(1)
	ctx := context.TODO()

	for b.Loop() {
		for range int(RunTimes) {
			go demoFunc(ctx)
		}
	}
}

func BenchmarkSemaphoreThroughput(b *testing.B) {
	runtime.GOMAXPROCS(1)
	ctx := context.TODO()

	sema := make(chan struct{}, PoolCap)
	for b.Loop() {
		for range int(RunTimes) {
			sema <- struct{}{}
			go func() {
				demoFunc(ctx)
				<-sema
			}()
		}
	}
}

func BenchmarkWorkerPoolThroughput(b *testing.B) {
	runtime.GOMAXPROCS(1)
	ctx := context.TODO()

	p := New(PoolCap)
	defer p.Close()

	for b.Loop() {
		for range int(RunTimes) {
			_ = p.Go(ctx, demoFunc)
		}
	}
}
