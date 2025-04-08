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
	base  *base
	queue string
	msgCh chan *taskMessage
}

func newWorker(base *base, queue string) *worker {
	return &worker{
		base:  base,
		queue: queue,
	}
}

func (w *worker) Start(wg *sync.WaitGroup) {
	workers := w.base.Matrix[w.queue]

	w.msgCh = make(chan *taskMessage, workers)

	w.startScheduled(wg)
	w.startPending(wg)
	w.startWorkers(wg, workers)
}

func (w *worker) startScheduled(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer w.base.Logger.Debug(fmt.Sprintf("qsync: stopped scheduled [queue=%s]", w.queue))
		w.base.Logger.Debug(fmt.Sprintf("qsync: started scheduled [queue=%s]", w.queue))
		for {
			select {
			case <-w.base.CloseCh:
				return
			default:
				if err := w.base.Broker.Scheduled(context.Background(), w.queue); err != nil {
					w.base.Logger.Error(fmt.Sprintf("qsync: fail scheduled [queue=%s]: %s", w.queue, err))
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
		defer w.base.Logger.Debug(fmt.Sprintf("qsync: stopped pending [queue=%s]", w.queue))
		w.base.Logger.Debug(fmt.Sprintf("qsync: started pending [queue=%s]", w.queue))
		for {
			select {
			case <-w.base.CloseCh:
				return
			default:
				msg, err := w.base.Broker.Dequeue(context.Background(), w.queue)
				if errors.Is(err, redis.Nil) {
					time.Sleep(time.Second)
					continue
				}
				if err != nil {
					w.base.Logger.Error(fmt.Sprintf("qsync: fail dequeue [queue=%s]: %s", w.queue, err))
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
			defer w.base.Logger.Debug(fmt.Sprintf("qsync: stopped worker [queue=%s]", w.queue))
			w.base.Logger.Debug(fmt.Sprintf("qsync: started worker [queue=%s]", w.queue))
			for msg := range w.msgCh {
				w.base.ProcessTask(w.queue, msg)
			}
		}()
	}
}
