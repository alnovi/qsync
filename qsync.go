package qsync

import (
	"context"
	"log/slog"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/alnovi/qsync/v2/utils"
)

const (
	Critical = "critical"
	Default  = "default"
	Lower    = "lower"
)

type Client interface {
	Enqueue(ctx context.Context, queue string, task *Task) error
}

type Server interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

type Qsync struct {
	broker *broker
	logger Logger
}

func New(client redis.UniversalClient, opts ...Option) (*Qsync, error) {
	qsync := &Qsync{
		broker: newBroker(client),
		logger: slog.New(slog.DiscardHandler),
	}

	for _, opt := range opts {
		if err := opt(qsync); err != nil {
			return nil, err
		}
	}

	return qsync, nil
}

func (q *Qsync) Ping(ctx context.Context) error {
	return q.broker.Ping(ctx)
}

func (q *Qsync) NewClient() Client {
	return newClient(q.broker)
}

func (q *Qsync) NewServer(mux *Mux, opts ...ServerOption) (Server, error) {
	return newServer(q.broker, mux, q.logger, opts...)
}

type Option func(q *Qsync) error

func WithPrefix(prefix string) Option {
	return func(q *Qsync) error {
		prefix = strings.TrimSpace(prefix)
		prefix = strings.ToLower(prefix)
		prefix += ":qsync"
		prefix = strings.Trim(prefix, ":")

		if utils.IsCluster(q.broker.client) {
			prefix += "{cluster}"
		}

		q.broker.prefix = prefix

		return nil
	}
}

func WithLogger(logger Logger) Option {
	return func(q *Qsync) error {
		if logger != nil {
			q.logger = logger
		}
		return nil
	}
}
