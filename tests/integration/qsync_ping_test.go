package integration

import "github.com/alnovi/qsync/v2"

func (s *TestSuite) TestQsyncPing() {
	testCases := []struct {
		name    string
		qsyncFn func() *qsync.Qsync
		expPing bool
	}{
		{
			name: "Success client",
			qsyncFn: func() *qsync.Qsync {
				queue, err := qsync.New(s.redisClient)
				s.Require().NoError(err, "failed to create queue")
				return queue
			},
			expPing: true,
		},
		{
			name: "Fail client",
			qsyncFn: func() *qsync.Qsync {
				queue, err := qsync.New(nil)
				s.Require().NoError(err, "failed to create queue")
				return queue
			},
			expPing: false,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.Require().NotNil(tc.qsyncFn)

			err := tc.qsyncFn().Ping(s.T().Context())
			if tc.expPing {
				s.Require().Nil(err, "ping failed")
			} else {
				s.Require().NotNil(err, "ping failed")
			}
		})
	}
}
