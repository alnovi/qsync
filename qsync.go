package qsync

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/alnovi/qsync/logger"
)

const (
	Critical = "critical"
	Default  = "default"
	Lower    = "lower"
)

var (
	ErrQueuesIsEmpty    = errors.New("queues is empty")
	ErrQueueNameIsEmpty = errors.New("queue name is empty")
	ErrQueueNotFound    = errors.New("queue not found")
	ErrMuxIsEmpty       = errors.New("mux is empty")
)

type Option func(q *Qsync) error

func WithPrefix(prefix string) Option {
	return func(q *Qsync) error {
		q.prefix = prefix
		return nil
	}
}

func WithMatrix(matrix map[string]uint) Option {
	return func(q *Qsync) error {
		q.base.Matrix = make(map[string]int, len(matrix))

		for k, v := range matrix {
			if k = strings.TrimSpace(k); k == "" {
				return ErrQueueNameIsEmpty
			}
			if v > 0 {
				q.base.Matrix[k] = int(v)
			}
		}

		if len(q.base.Matrix) == 0 {
			return ErrQueuesIsEmpty
		}

		return nil
	}
}

func WithLogger(logger logger.Logger) Option {
	return func(q *Qsync) error {
		if logger != nil {
			q.base.Logger = logger
		}
		return nil
	}
}

func WithContext(fn func() context.Context) Option {
	return func(q *Qsync) error {
		if fn != nil && fn() != nil {
			q.base.BaseCtxFn = fn
		}
		return nil
	}
}

func WithErrorHandler(fn func(error, *TaskInfo)) Option {
	return func(q *Qsync) error {
		if fn != nil {
			q.base.ErrHandle = fn
		}
		return nil
	}
}

type Qsync struct {
	prefix    string
	base      *base
	processor *processor
	mu        sync.Mutex
}

func New(client redis.UniversalClient, opts ...Option) (*Qsync, error) {
	q := &Qsync{base: newState()}
	if err := q.applyOptions(opts); err != nil {
		return nil, err
	}

	q.base.Broker = newBroker(q.prefix, client)
	q.processor = newProcessor(q.base)

	return q, nil
}

func (q *Qsync) Ping(ctx context.Context) error {
	return q.base.Broker.Ping(ctx)
}

func (q *Qsync) Enqueue(ctx context.Context, queue string, task *Task) error {
	if !q.base.HasMatrixQueue(queue) {
		return ErrQueueNotFound
	}

	msg, err := newTaskMessage(task)
	if err != nil {
		return err
	}

	return q.base.Broker.Enqueue(ctx, queue, msg)
}

func (q *Qsync) Start(mux *Mux) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if mux == nil {
		return ErrMuxIsEmpty
	}

	if q.base.IsRunning {
		return nil
	}

	q.base.IsRunning = true
	q.base.Mux = mux

	q.processor.Start()

	return nil
}

func (q *Qsync) Stop() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.base.IsRunning {
		return
	}

	q.base.IsRunning = false
	q.processor.Stop()
}

func (q *Qsync) applyOptions(opts []Option) error {
	for _, opt := range opts {
		if err := opt(q); err != nil {
			return err
		}
	}
	return nil
}
