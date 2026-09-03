package qsync

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	subsystem     = "qsync"
	statusOK      = "ok"
	statusErr     = "error"
	statusExpired = "expired"
)

type Metrics struct {
	enabled           bool
	namespace         string
	register          prometheus.Registerer
	queueEnqueueCount *prometheus.CounterVec
	queueDequeueCount *prometheus.CounterVec
	taskProcessCount  *prometheus.CounterVec
}

// nolint:goconst
func NewMetrics(enabled bool, opts ...MetricsOption) *Metrics {
	m := &Metrics{
		enabled:   enabled,
		namespace: "",
	}

	for _, opt := range opts {
		opt(m)
	}

	if !m.enabled {
		return m
	}

	if m.register == nil {
		m.register = prometheus.DefaultRegisterer
	}

	m.queueEnqueueCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: m.namespace,
		Subsystem: subsystem,
		Name:      "queue_enqueue_count",
		Help:      "Number of enqueue requests",
	}, []string{"status", "queue", "task"})
	m.register.MustRegister(m.queueEnqueueCount)

	m.queueDequeueCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: m.namespace,
		Subsystem: subsystem,
		Name:      "queue_dequeue_count",
		Help:      "Number of dequeue requests",
	}, []string{"status", "queue"})
	m.register.MustRegister(m.queueDequeueCount)

	m.taskProcessCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: m.namespace,
		Subsystem: subsystem,
		Name:      "task_process_count",
		Help:      "Number of task processed",
	}, []string{"status", "queue", "task"})
	m.register.MustRegister(m.taskProcessCount)

	return m
}

func (m *Metrics) QueueEnqueueOkInc(queue, task string) {
	if m.enabled {
		m.queueEnqueueCount.With(prometheus.Labels{"status": statusOK, "queue": queue, "task": task}).Inc()
	}
}

func (m *Metrics) QueueEnqueueErrInc(queue, task string) {
	if m.enabled {
		m.queueEnqueueCount.With(prometheus.Labels{"status": statusErr, "queue": queue, "task": task}).Inc()
	}
}

func (m *Metrics) QueueDequeueOkInc(queue string) {
	if m.enabled {
		m.queueDequeueCount.With(prometheus.Labels{"status": statusOK, "queue": queue}).Inc()
	}
}

func (m *Metrics) QueueDequeueErrInc(queue string) {
	if m.enabled {
		m.queueDequeueCount.With(prometheus.Labels{"status": statusErr, "queue": queue}).Inc()
	}
}

func (m *Metrics) TaskProcessOkInc(queue string, task *taskMessage) {
	if m.enabled {
		m.taskProcessCount.With(prometheus.Labels{"status": statusOK, "queue": queue, "task": task.Type}).Inc()
	}
}

func (m *Metrics) TaskProcessErrInc(queue string, task *taskMessage) {
	if m.enabled {
		if m.enabled {
			m.taskProcessCount.With(prometheus.Labels{"status": statusErr, "queue": queue, "task": task.Type}).Inc()
		}
	}
}

func (m *Metrics) TaskProcessExpiredInc(queue string, task *taskMessage) {
	if m.enabled {
		m.taskProcessCount.With(prometheus.Labels{"status": statusExpired, "queue": queue, "task": task.Type}).Inc()
	}
}

type MetricsOption func(*Metrics)

func WithEnabled(enabled bool) MetricsOption {
	return func(m *Metrics) {
		m.enabled = enabled
	}
}

func WithNamespace(namespace string) MetricsOption {
	return func(m *Metrics) {
		m.namespace = namespace
	}
}

func WithRegister(register prometheus.Registerer) MetricsOption {
	return func(m *Metrics) {
		if register != nil {
			m.register = register
		}
	}
}
