package integration

import (
	"github.com/alnovi/qsync/v2"
)

func (s *TestSuite) TestMetrics() {
	s.Require().NotPanics(func() {
		opts := []qsync.MetricsOption{
			qsync.WithEnabled(true),
			qsync.WithNamespace("testing"),
			qsync.WithRegister(nil),
		}

		m := qsync.NewMetrics(true, opts...)

		m.QueueEnqueueOkInc(qsync.Default, "task-name")
		m.QueueEnqueueErrInc(qsync.Default, "task-name")

		m.QueueDequeueOkInc(qsync.Default)
		m.QueueDequeueErrInc(qsync.Default)

		taskMsg, _ := qsync.NewTaskMessage(qsync.NewTask("task-name", nil))

		m.TaskProcessOkInc(qsync.Default, taskMsg)
		m.TaskProcessErrInc(qsync.Default, taskMsg)
		m.TaskProcessExpiredInc(qsync.Default, taskMsg)
	})
}
