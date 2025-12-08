package qsync

import (
	"context"
	"errors"
)

type client struct {
	broker  *broker
	metrics *Metrics
}

func newClient(broker *broker, metrics *Metrics) *client {
	return &client{broker: broker, metrics: metrics}
}

func (c *client) Enqueue(ctx context.Context, queue string, task *Task) error {
	msg, err := newTaskMessage(task)
	if err != nil {
		c.metrics.QueueEnqueueErrInc(queue, task.typename)
		return err
	}

	err = c.broker.Enqueue(ctx, queue, msg)
	if err != nil {
		if !errors.Is(err, ErrTaskIsExists) {
			c.metrics.QueueEnqueueErrInc(queue, task.typename)
		}
		return err
	}

	c.metrics.QueueEnqueueOkInc(queue, task.typename)

	return nil
}
