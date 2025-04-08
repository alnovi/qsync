package qsync

import (
	"context"
	"fmt"
	"time"

	"github.com/alnovi/qsync/logger"
)

var (
	defaultMatrix = map[string]int{
		Critical: 6,
		Default:  3,
		Lower:    1,
	}
)

type base struct {
	IsRunning bool
	Logger    logger.Logger
	Matrix    map[string]int
	BaseCtxFn func() context.Context
	ErrHandle func(error, *TaskInfo)
	Broker    *broker
	Mux       *Mux
	CloseCh   chan struct{}
}

func newState() *base {
	return &base{
		IsRunning: false,
		Logger:    logger.NewStubLogger(),
		Matrix:    defaultMatrix,
		BaseCtxFn: context.Background,
		ErrHandle: nil,
	}
}

func (b *base) HasMatrixQueue(queue string) bool {
	_, ok := b.Matrix[queue]
	return ok
}

func (b *base) ProcessTask(queue string, msg *taskMessage) {
	defer func() {
		if err := recover(); err != nil {
			b.markTaskError(queue, msg, err.(error))
		}
	}()

	if err := msg.IsDeadline(); err != nil {
		b.markTaskDeadline(msg, err)
		return
	}

	handle, err := b.Mux.Handle(msg.Type)
	if err != nil {
		b.markTaskError(queue, msg, err)
		return
	}

	err = handle(b.BaseCtxFn(), newTaskInfo(msg))
	if err != nil {
		b.markTaskError(queue, msg, err)
		return
	}

	b.markTaskSuccess(msg)
}

func (b *base) markTaskDeadline(msg *taskMessage, err error) {
	b.runErrHandler(err, newTaskInfo(msg))
}

func (b *base) markTaskSuccess(_ *taskMessage) {}

func (b *base) markTaskError(queue string, msg *taskMessage, err error) {
	b.runErrHandler(err, newTaskInfo(msg))

	if msg.Retry > msg.Retried {
		msg.Retried++
		msg.ProcessAt = time.Now().Add(msg.RetryDelay)
		if err = b.Broker.Enqueue(context.Background(), queue, msg); err != nil {
			b.Logger.Error(fmt.Sprintf("qsync: fail enqueue retry [key=%s]: %s", msg.Key(), err))
		}
	}
}

func (b *base) runErrHandler(err error, task *TaskInfo) {
	if b.ErrHandle != nil {
		go b.ErrHandle(err, task)
	}
}
