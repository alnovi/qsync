package integration

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/alnovi/qsync/v2"
)

func (s *TestSuite) TestQueueDefault() {
	const taskType = "test-task"

	testCases := []struct {
		name    string
		sleep   time.Duration
		tasksFn func() []*qsync.Task
		failCnt int
		expExec int
	}{
		{
			name:  "Success",
			sleep: 5 * time.Second,
			tasksFn: func() []*qsync.Task {
				return []*qsync.Task{
					qsync.NewTask(taskType, nil),
				}
			},
			failCnt: 0,
			expExec: 1,
		},
		{
			name:  "Success duplicates",
			sleep: 3 * time.Minute,
			tasksFn: func() []*qsync.Task {
				return []*qsync.Task{
					qsync.NewTask(taskType, nil, qsync.WithId("id"), qsync.WithRetry(1)),
					qsync.NewTask(taskType, nil, qsync.WithId("id"), qsync.WithRetry(3)),
					qsync.NewTask(taskType, nil, qsync.WithId("id"), qsync.WithRetry(5)),
				}
			},
			failCnt: 0,
			expExec: 1,
		},
		{
			name:  "Success delay",
			sleep: time.Minute,
			tasksFn: func() []*qsync.Task {
				return []*qsync.Task{
					qsync.NewTask(taskType, nil, qsync.WithDelay(time.Second)),
					qsync.NewTask(taskType, nil, qsync.WithDelay(time.Hour)),
				}
			},
			failCnt: 0,
			expExec: 1,
		},
		{
			name:  "Success retry",
			sleep: time.Minute,
			tasksFn: func() []*qsync.Task {
				return []*qsync.Task{
					qsync.NewTask(taskType, nil, qsync.WithRetry(10), qsync.WithRetryDelay(time.Second)),
				}
			},
			failCnt: 9,
			expExec: 10,
		},
		{
			name:  "Success retry delay",
			sleep: 5 * time.Minute,
			tasksFn: func() []*qsync.Task {
				return []*qsync.Task{
					qsync.NewTask(taskType, nil, qsync.WithRetry(10), qsync.WithRetryDelay(time.Minute)),
				}
			},
			failCnt: 10,
			expExec: 5,
		},
		{
			name:  "Success process_at",
			sleep: 30 * time.Minute,
			tasksFn: func() []*qsync.Task {
				return []*qsync.Task{
					qsync.NewTask(taskType, nil, qsync.WithProcessAt(time.Now().Add(10*time.Minute))),
					qsync.NewTask(taskType, nil, qsync.WithProcessAt(time.Now().Add(20*time.Minute))),
					qsync.NewTask(taskType, nil, qsync.WithProcessAt(time.Now().Add(40*time.Minute))),
				}
			},
			failCnt: 0,
			expExec: 2,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.redisClient.FlushAll(s.T().Context())
			synctest.Test(s.T(), func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()

				actualExec := 0

				handler := func(_ context.Context, task *qsync.TaskInfo) error {
					actualExec++
					if actualExec <= tc.failCnt {
						return errors.New("fail task")
					}
					return nil
				}

				queue, err := qsync.New(s.redisClient)
				s.Require().NoError(err, "failed to initialize queue")

				mux := qsync.NewMux()
				err = mux.HandleFunc(taskType, handler)
				s.Require().NoError(err, "failed to register task")

				serverOpts := []qsync.ServerOption{
					qsync.WithMatrix(map[string]int{qsync.Critical: 3, qsync.Default: 2, qsync.Lower: 1}),
					qsync.WithContext(s.T().Context),
					qsync.WithErrorHandler(func(_ error, _ *qsync.TaskInfo) {}),
					qsync.WithServerLogger(slog.New(slog.DiscardHandler)),
					qsync.WithWait(time.Second),
				}

				server, err := queue.NewServer(mux, serverOpts...)
				s.Require().NoError(err, "failed to initialize server")

				client := queue.NewClient()
				for _, task := range tc.tasksFn() {
					_ = client.Enqueue(ctx, qsync.Default, task)
				}

				err = server.Start(ctx)
				s.Require().NoError(err, "failed to start server")

				time.Sleep(tc.sleep)
				synctest.Wait()

				err = server.Stop(ctx)
				s.Require().NoError(err, "failed to stop server")

				time.Sleep(time.Minute)
				synctest.Wait()

				s.Assert().Equal(tc.expExec, actualExec, "not equal expected task executed")
			})
		})
	}
}
