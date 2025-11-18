package qsync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type worker struct {
	broker  *broker
	workers int
	queue   string
	logger  Logger
	msgCh   chan *taskMessage
	closeCh <-chan struct{}
	process func(queue string, msg *taskMessage)
}

func newWorker(broker *broker, queue string, workers int, logger Logger, process func(queue string, msg *taskMessage)) *worker {
	return &worker{
		broker:  broker,
		queue:   queue,
		workers: workers,
		logger:  logger,
		process: process,
	}
}

func (w *worker) Start(_ context.Context, wg *sync.WaitGroup, closeCh <-chan struct{}) {
	w.msgCh = make(chan *taskMessage, w.workers)
	w.closeCh = closeCh
	w.startScheduled(wg)
	w.startPending(wg)
	w.startWorkers(wg, w.workers)
}

func (w *worker) startScheduled(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer w.logger.Debug(fmt.Sprintf("qsync-server: stopped scheduled [queue=%s]", w.queue))
		w.logger.Debug(fmt.Sprintf("qsync-server: started scheduled [queue=%s]", w.queue))
		for {
			select {
			case <-w.closeCh:
				return
			default:
				if err := w.broker.Scheduled(context.Background(), w.queue); err != nil {
					w.logger.Error(fmt.Sprintf("qsync-server: fail scheduled [queue=%s]: %s", w.queue, err))
				}
				time.Sleep(time.Second)
			}
		}
	}()
}

func (w *worker) startPending(wg *sync.WaitGroup) {
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
				msg, err := w.broker.Dequeue(context.Background(), w.queue)
				if errors.Is(err, redis.Nil) {
					time.Sleep(time.Second)
					continue
				}
				if err != nil {
					w.logger.Error(fmt.Sprintf("qsync-server: fail dequeue [queue=%s]: %s", w.queue, err))
					time.Sleep(time.Second)
					continue
				}
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
