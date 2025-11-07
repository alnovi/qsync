package qsync

import "context"

type client struct {
	broker *broker
}

func newClient(broker *broker) *client {
	return &client{broker: broker}
}

func (c *client) Enqueue(ctx context.Context, queue string, task *Task) error {
	msg, err := newTaskMessage(task)
	if err != nil {
		return err
	}

	return c.broker.Enqueue(ctx, queue, msg)
}
