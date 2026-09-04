package integration

import (
	"time"

	"github.com/alnovi/qsync/v2"
)

func (s *TestSuite) TestTaskMessage() {
	testCases := []struct {
		name   string
		taskFn func() *qsync.Task
		expErr string
	}{
		{
			name: "Success",
			taskFn: func() *qsync.Task {
				return qsync.NewTask("task-name", nil)
			},
			expErr: "",
		},
		{
			name: "Task is nil",
			taskFn: func() *qsync.Task {
				return nil
			},
			expErr: "task is nil",
		},
		{
			name: "Task type invalid",
			taskFn: func() *qsync.Task {
				return &qsync.Task{}
			},
			expErr: "task type is empty",
		},
		{
			name: "Task max retry",
			taskFn: func() *qsync.Task {
				return qsync.NewTask("task-name", nil, qsync.WithRetry(1000))
			},
			expErr: "",
		},
		{
			name: "Task deadlineAt",
			taskFn: func() *qsync.Task {
				return qsync.NewTask("task-name", nil, qsync.WithDeadline(time.Now().Add(time.Hour)))
			},
			expErr: "",
		},
		{
			name: "Task processAt",
			taskFn: func() *qsync.Task {
				return qsync.NewTask("task-name", nil, qsync.WithProcessAt(time.Now().Add(time.Hour)))
			},
			expErr: "",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			_, err := qsync.NewTaskMessage(tc.taskFn())
			if tc.expErr == "" {
				s.Require().NoError(err, "create msg return error")
			} else {
				s.Require().Error(err, "create msg not return error")
				s.Require().ErrorContains(err, tc.expErr, "create msg error not equal")
			}
		})
	}
}
