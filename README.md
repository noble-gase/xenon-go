# xenon-go

[![golang](https://img.shields.io/badge/Language-Go-green.svg?style=flat)](https://golang.org)
[![pkg.go.dev](https://img.shields.io/badge/dev-reference-007d9c?logo=go&logoColor=white&style=flat)](https://pkg.go.dev/github.com/noble-gase/xenon)
[![MIT](http://img.shields.io/badge/license-MIT-brightgreen.svg)](http://opensource.org/licenses/MIT)

[氙-Xenon] Go协程并发复用，降低CPU和内存负载

## 安装

```shell
go get -u github.com/noble-gase/xenon
```

| 模块      | 说明                        |
| --------- | --------------------------- |
| errgroup  | 控制协程数量并按需创建      |
| timewheel | 分层时间轮（任务支持重试）  |
| worker    | 基于 `channel` 实现的协程池 |

## 流程图

![flowchart.jpg](example/flowchart.jpg)

## 效果

```shell
goos: darwin
goarch: arm64
cpu: Apple M4
```

### 场景-1

```go
import "github.com/noble-gase/xenon/worker"

func main() {
    ctx := context.Background()

    pool := worker.New(5000)
    for i := 0; i < 100000000; i++ {
        i := i
        pool.Go(ctx, func(ctx context.Context) {
            time.Sleep(time.Second)
            fmt.Println("Index:", i)
        })
    }

    <-ctx.Done()
}
```

#### cpu

![cpu-1.png](example/cpu-1.png)

#### mem

![mem-1.png](example/mem-1.png)

### 场景-2

```go
import "github.com/noble-gase/xenon/worker"

func main() {
    ctx := context.Background()

    pool := worker.New(5000)
    for i := 0; i < 100; i++ {
        i := i
        pool.Go(ctx, func(ctx context.Context) {
            for j := 0; j < 1000000; j++ {
                j := j
                pool.Go(ctx, func(ctx context.Context) {
                    time.Sleep(time.Second)
                    fmt.Println("Index:", i, "-", j)
                })
            }
        })
    }

    <-ctx.Done()
}
```

#### cpu

![cpu-2.png](example/cpu-2.png)

#### mem

![mem-2.png](example/mem-2.png)
