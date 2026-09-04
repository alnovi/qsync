package integration

import (
	"log/slog"
	"time"

	"github.com/alnovi/qsync/v2"
)

func (s *TestSuite) TestClientEnqueue() {
	testCases := []struct {
		name   string
		tasks  []*qsync.Task
		expErr string
	}{
		{
			name: "Success enqueue 1 task",
			tasks: []*qsync.Task{
				qsync.NewTask("test-task", nil, qsync.WithDelay(time.Minute)),
			},
			expErr: "",
		},
		{
			name: "Success enqueue 2 task",
			tasks: []*qsync.Task{
				qsync.NewTask("test-task", nil, qsync.WithDelay(time.Minute)),
				qsync.NewTask("test-task", nil, qsync.WithDelay(time.Minute)),
			},
			expErr: "",
		},
		{
			name: "Blocked enqueue 2 task",
			tasks: []*qsync.Task{
				qsync.NewTask("test-task", nil, qsync.WithId("task_id")),
				qsync.NewTask("test-task", nil, qsync.WithId("task_id")),
			},
			expErr: "task is exists",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			opts := []qsync.Option{
				qsync.WithPrefix("testing"),
				qsync.WithLogger(slog.New(slog.DiscardHandler)),
				qsync.WithMetrics(qsync.WithEnabled(false)),
			}

			queue, err := qsync.New(s.redisClient, opts...)
			s.Require().NoError(err, "failed to create queue")

			for _, task := range tc.tasks {
				err = queue.NewClient().Enqueue(s.T().Context(), qsync.Default, task)
			}

			if tc.expErr == "" {
				s.Require().NoError(err, "enqueue task was successful")
			} else {
				s.Require().Error(err, "enqueue task was failure")
				s.Assert().ErrorContains(err, tc.expErr, "enqueue task error not equal")
			}
		})
	}
}
