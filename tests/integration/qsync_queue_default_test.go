package integration

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/alnovi/qsync/v2"
)

func (s *TestSuite) TestQSyncQueueDefault() {
	const taskType = "test-task"

	testCases := []struct {
		name    string
		sleep   time.Duration
		task    *qsync.Task
		failCnt int
		expExec int
	}{
		{
			name:    "Success",
			sleep:   5 * time.Second,
			task:    qsync.NewTask(taskType, nil),
			failCnt: 0,
			expExec: 1,
		},
		{
			name:    "Success retry",
			sleep:   3 * time.Minute,
			task:    qsync.NewTask(taskType, nil, qsync.WithRetry(3), qsync.WithRetryDelay(time.Minute)),
			failCnt: 2,
			expExec: 3,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
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

				server, err := queue.NewServer(mux)
				s.Require().NoError(err, "failed to initialize server")

				client := queue.NewClient()
				err = client.Enqueue(ctx, qsync.Default, tc.task)
				s.Require().NoError(err, "failed to enqueue task")

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
