package integration

import (
	"context"

	"github.com/alnovi/qsync/v2"
)

type Handler struct {
	name    string
	handler func(ctx context.Context, task *qsync.TaskInfo) error
}

func (h *Handler) NameTask() string {
	return h.name
}

func (h *Handler) ProcessTask(ctx context.Context, task *qsync.TaskInfo) error {
	return h.handler(ctx, task)
}

func (s *TestSuite) NewHandlerStub(name string) *Handler {
	s.T().Helper()
	return &Handler{name: name, handler: func(ctx context.Context, task *qsync.TaskInfo) error { return nil }}
}
