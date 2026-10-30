package concurrency

import "time"

type BatchingChannel[T any] interface {
	In() chan<- T
	Out() <-chan []T
}

type batchingChannel[T any] struct {
	input        chan T
	output       chan []T
	buffer       []T
	maxBatchSize int
	maxWaitTime  time.Duration
	timer        *time.Timer
}

func NewBatchingChannel[T any](capacity, maxBatchSize int, maxWaitTime time.Duration) BatchingChannel[T] {
	if capacity < 1 {
		capacity = 1
	}
	if maxBatchSize < 1 {
		maxBatchSize = 1
	}
	if maxWaitTime <= 0 {
		maxWaitTime = time.Millisecond
	}
	ch := &batchingChannel[T]{
		input:        make(chan T, capacity),
		output:       make(chan []T, 1),
		maxBatchSize: maxBatchSize,
		maxWaitTime:  maxWaitTime,
		timer:        time.NewTimer(maxWaitTime),
	}
	go ch.batch()
	return ch
}

func (ch *batchingChannel[T]) In() chan<- T {
	return ch.input
}

func (ch *batchingChannel[T]) Out() <-chan []T {
	return ch.output
}

func (ch *batchingChannel[T]) batch() {
	defer close(ch.output)
	defer ch.timer.Stop()
	for {
		select {
		case next, ok := <-ch.input:
			if !ok {
				if len(ch.buffer) > 0 {
					ch.flush()
				}
				return
			}
			ch.buffer = append(ch.buffer, next)
			if len(ch.buffer) >= ch.maxBatchSize {
				ch.flush()
			}
		case <-ch.timer.C:
			if len(ch.buffer) > 0 {
				ch.flush()
			} else {
				ch.resetTimer()
			}
		}
	}
}

func (ch *batchingChannel[T]) flush() {
	batch := ch.buffer
	ch.buffer = nil
	ch.resetTimer()
	ch.output <- batch
}

func (ch *batchingChannel[T]) resetTimer() {
	if !ch.timer.Stop() {
		select {
		case <-ch.timer.C:
		default:
		}
	}
	ch.timer.Reset(ch.maxWaitTime)
}
