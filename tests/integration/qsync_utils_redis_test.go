package integration

import (
	"github.com/redis/go-redis/v9"

	"github.com/alnovi/qsync/v2/utils"
)

func (s *TestSuite) TestQsyncUtilsRedis() {
	testCases := []struct {
		name   string
		client redis.UniversalClient
		expRes bool
	}{
		{
			name:   "Redis client",
			client: s.redisClient,
			expRes: false,
		},
		{
			name:   "Redis client nil",
			client: nil,
			expRes: false,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			actRes := utils.IsCluster(tc.client)
			s.Assert().Equal(tc.expRes, actRes, "IsCluster() not equal")
		})
	}
}
