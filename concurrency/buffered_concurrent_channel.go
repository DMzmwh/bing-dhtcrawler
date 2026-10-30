package concurrency

import (
	"context"
	"sync"
)

// BufferedConcurrentChannel 带缓冲输入通道，固定并发worker消费
// 契约：发送方完成数据投递之后必须调用 close(In())；否则Run会永久阻塞
// 注意：业务回调f如果是长耗时逻辑，f内部需要自行监听ctx，worker无法中断正在运行的f
type BufferedConcurrentChannel[T any] interface {
	In() chan<- T
	Run(ctx context.Context, f func(T)) error
}

func NewBufferedConcurrentChannel[T any](capacity, concurrency int) BufferedConcurrentChannel[T] {
	if capacity < 1 {
		capacity = 1
	}
	if concurrency < 1 {
		concurrency = 1
	}
	return &bufferedConcurrentChannel[T]{
		ch:          make(chan T, capacity),
		concurrency: concurrency,
	}
}

type bufferedConcurrentChannel[T any] struct {
	ch          chan T
	concurrency int
}

func (ch *bufferedConcurrentChannel[T]) In() chan<- T {
	return ch.ch
}

func (ch *bufferedConcurrentChannel[T]) Run(ctx context.Context, f func(T)) error {
	// 如果ctx已经提前取消，直接返回，不再启动worker
	if err := ctx.Err(); err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(ch.concurrency)

	for i := 0; i < ch.concurrency; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case next, ok := <-ch.ch:
					if !ok {
						return
					}
					f(next)
				}
			}
		}()
	}

	wg.Wait()
	return ctx.Err()
}
