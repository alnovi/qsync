package integration

import (
	"context"
	"net"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	conRedis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/alnovi/qsync/v2/utils"
)

const (
	RedisImage    = "redis:8-alpine3.23"
	RedisPassCost = 8
)

type TestSuite struct {
	suite.Suite
	redisContainer *conRedis.RedisContainer
	redisClient    redis.UniversalClient
}

func TestIntegration(t *testing.T) {
	suite.Run(t, new(TestSuite))
}

func (s *TestSuite) SetupSuite() {
	s.initRedis(s.T().Context())
}

func (s *TestSuite) TearDownSuite() {
	if s.redisContainer != nil {
		s.NoError(s.redisContainer.Terminate(s.T().Context()))
	}
}

func (s *TestSuite) SetupTest() {
	if s.redisClient != nil {
		s.redisClient.FlushAll(s.T().Context())
	}
}

func (s *TestSuite) TearDownTest() {}

func (s *TestSuite) initRedis(ctx context.Context) {
	s.T().Helper()

	var err error

	password := utils.MustRandBase62(RedisPassCost)

	s.redisContainer, err = conRedis.Run(ctx, RedisImage,
		conRedis.WithLogLevel(conRedis.LogLevelVerbose),
		testcontainers.WithEnv(map[string]string{
			"REDIS_PASSWORD": password,
		}),
	)
	s.Require().NoError(err)

	host, err := s.redisContainer.Host(ctx)
	s.Require().NoError(err)

	port, err := s.redisContainer.MappedPort(ctx, "6379/tcp")
	s.Require().NoError(err)

	s.redisClient = redis.NewClient(&redis.Options{
		Addr:       net.JoinHostPort(host, port.Port()),
		Password:   password,
		ClientName: "qsync",
	})

	err = s.redisClient.Ping(ctx).Err()
	s.Require().NoError(err)
}
