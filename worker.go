package qsync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type worker struct {
	broker  *broker
	queue   string
	workers int
	wait    time.Duration // Schedule wait timeout
	logger  *slog.Logger
	metrics *Metrics
	msgCh   chan *TaskMessage
	closeCh <-chan struct{}
	process func(queue string, msg *TaskMessage)
}

func newWorker(broker *broker, queue string, workers int, wait time.Duration, logger *slog.Logger, metrics *Metrics, process func(queue string, msg *TaskMessage)) *worker {
	return &worker{
		broker:  broker,
		queue:   queue,
		workers: workers,
		wait:    wait,
		logger:  logger,
		metrics: metrics,
		process: process,
	}
}

func (w *worker) Start(ctx context.Context, wg *sync.WaitGroup, closeCh <-chan struct{}) {
	w.msgCh = make(chan *TaskMessage, w.workers)
	w.closeCh = closeCh
	w.startScheduled(ctx, wg)
	w.startPending(ctx, wg)
	w.startWorkers(wg, w.workers)
}

func (w *worker) startScheduled(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer w.logger.Debug(fmt.Sprintf("qsync-server: stopped scheduled [queue=%s]", w.queue))
		w.logger.Debug(fmt.Sprintf("qsync-server: started scheduled [queue=%s]", w.queue))
		for {
			select {
			case <-w.closeCh:
				return
			case <-time.After(w.wait):
				if err := w.broker.Scheduled(ctx, w.queue); err != nil {
					w.logger.Error(fmt.Sprintf("qsync-server: fail scheduled [queue=%s]: %s", w.queue, err))
					select {
					case <-w.closeCh:
						return
					case <-time.After(time.Minute):
						continue
					}
				}
			}
		}
	}()
}

func (w *worker) startPending(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(w.msgCh)
		defer w.logger.Debug(fmt.Sprintf("qsync-server: stopped pending [queue=%s]", w.queue))
		w.logger.Debug(fmt.Sprintf("qsync-server: started pending [queue=%s]", w.queue))
		for {
			select {
			case <-w.closeCh:
				return
			default:
				msg, err := w.broker.Dequeue(ctx, w.queue)
				if errors.Is(err, redis.Nil) {
					select {
					case <-w.closeCh:
						return
					case <-time.After(time.Second):
						continue
					}
				}
				if err != nil {
					w.logger.Error(fmt.Sprintf("qsync-server: fail dequeue [queue=%s]: %s", w.queue, err))
					w.metrics.QueueDequeueErrInc(w.queue)
					select {
					case <-w.closeCh:
						return
					case <-time.After(time.Minute):
						continue
					}
				}
				w.metrics.QueueDequeueOkInc(w.queue)
				w.msgCh <- msg
			}
		}
	}()
}

func (w *worker) startWorkers(wg *sync.WaitGroup, n int) {
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			defer w.logger.Debug(fmt.Sprintf("qsync-server: stopped worker [queue=%s]", w.queue))
			w.logger.Debug(fmt.Sprintf("qsync-server: started worker [queue=%s]", w.queue))
			for msg := range w.msgCh {
				w.process(w.queue, msg)
			}
		}()
	}
}
