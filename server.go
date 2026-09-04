package qsync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

var (
	ErrMuxIsEmpty    = errors.New("mux is empty")
	ErrMatrixIsEmpty = errors.New("matrix is empty")

	defaultMatrix = map[string]int{
		Critical: 5, // nolint:mnd
		Default:  3, // nolint:mnd
		Lower:    1, // nolint:mnd
	}
)

type server struct {
	broker    *broker
	mux       *Mux
	matrix    map[string]int
	wait      time.Duration
	errHandle func(error, *TaskInfo)
	ctxFn     func() context.Context
	logger    *slog.Logger
	metrics   *Metrics
	mu        sync.Mutex
	wg        sync.WaitGroup
	isRun     bool
	closeCh   chan struct{}
}

func newServer(broker *broker, mux *Mux, log *slog.Logger, metrics *Metrics, opts ...ServerOption) (*server, error) {
	s := &server{
		broker:    broker,
		mux:       mux,
		matrix:    defaultMatrix,
		wait:      time.Second,
		errHandle: func(err error, taskInfo *TaskInfo) {},
		ctxFn:     context.Background,
		logger:    log,
		metrics:   metrics,
		isRun:     false,
	}

	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (s *server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mux == nil {
		return ErrMuxIsEmpty
	}

	if s.isRun {
		return nil
	}

	s.isRun = true
	s.closeCh = make(chan struct{})

	for queue, workers := range s.matrix {
		newWorker(s.broker, queue, workers, s.wait, s.logger, s.metrics, s.processTask).Start(ctx, &s.wg, s.closeCh)
	}

	return nil
}

func (s *server) Stop(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isRun {
		return nil
	}

	close(s.closeCh)
	s.wg.Wait()
	s.isRun = false

	return nil
}

func (s *server) processTask(queue string, msg *TaskMessage) {
	defer func() {
		if err := recover(); err != nil {
			s.markTaskError(queue, msg, err.(error))
		}
	}()

	if err := msg.IsDeadline(); err != nil {
		s.markTaskDeadline(queue, msg, err)
		return
	}

	handle, err := s.mux.Handle(msg.Type)
	if err != nil {
		s.markTaskError(queue, msg, err)
		return
	}

	err = handle(s.ctxFn(), newTaskInfo(msg))
	if err != nil {
		s.markTaskError(queue, msg, err)
		return
	}

	s.markTaskSuccess(queue, msg)
}

func (s *server) runErrHandler(err error, task *TaskInfo) {
	go s.errHandle(err, task)
}

func (s *server) markTaskError(queue string, msg *TaskMessage, err error) {
	s.metrics.TaskProcessErrInc(queue, msg)
	s.runErrHandler(err, newTaskInfo(msg))

	if msg.Retry > msg.Retried {
		msg.Retried++
		msg.ProcessAt = time.Now().Add(msg.RetryDelay)
		if err = s.broker.Enqueue(context.Background(), queue, msg); err != nil {
			s.logger.Error(fmt.Sprintf("qsync-server: fail enqueue retry [key=%s]: %s", msg.Key(), err))
		}
	}
}

func (s *server) markTaskDeadline(queue string, msg *TaskMessage, err error) {
	s.metrics.TaskProcessExpiredInc(queue, msg)
	s.runErrHandler(err, newTaskInfo(msg))
}

func (s *server) markTaskSuccess(queue string, msg *TaskMessage) {
	s.metrics.TaskProcessOkInc(queue, msg)
}

type ServerOption func(s *server) error

func WithMatrix(matrix map[string]int) ServerOption {
	return func(s *server) error {
		if matrix != nil {
			s.matrix = map[string]int{}
			for queue, workers := range matrix {
				if workers > 0 {
					s.matrix[queue] = workers
				}
			}
		}

		if len(s.matrix) == 0 {
			return ErrMatrixIsEmpty
		}

		return nil
	}
}

func WithContext(fn func() context.Context) ServerOption {
	return func(s *server) error {
		if fn != nil {
			s.ctxFn = fn
		}
		return nil
	}
}

func WithErrorHandler(fn func(error, *TaskInfo)) ServerOption {
	return func(s *server) error {
		if fn != nil {
			s.errHandle = fn
		}
		return nil
	}
}

func WithServerLogger(logger *slog.Logger) ServerOption {
	return func(s *server) error {
		if logger != nil {
			s.logger = logger
		}
		return nil
	}
}

func WithWait(wait time.Duration) ServerOption {
	return func(s *server) error {
		if wait > 0 {
			s.wait = wait
		}
		return nil
	}
}
