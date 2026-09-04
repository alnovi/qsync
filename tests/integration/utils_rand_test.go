package integration

import "github.com/alnovi/qsync/v2/utils"

func (s *TestSuite) TestUtilsRandBase62() {
	testCases := []struct {
		name      string
		length    int
		expLength int
		expError  string
	}{
		{
			name:      "Success 8 length",
			length:    8,
			expLength: 8,
			expError:  "",
		},
		{
			name:      "Success 0 length",
			length:    0,
			expLength: 0,
			expError:  "length must be positive",
		},
		{
			name:      "Success -10 length",
			length:    -10,
			expLength: 0,
			expError:  "length must be positive",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			value, err := utils.RandBase62(tc.length)
			if tc.expError == "" {
				s.Require().NoError(err)
			} else {
				s.Require().Error(err)
				s.Require().ErrorContains(err, tc.expError, "error should contain expected error")
			}

			s.Assert().Len(value, tc.expLength, "len result not as expected")
		})
	}
}

func (s *TestSuite) TestQsyncUtilsMustRandBase62() {
	s.Assert().NotPanics(func() {
		_ = utils.MustRandBase62(8)
	})

	s.Assert().Panics(func() {
		_ = utils.MustRandBase62(0)
	})
}
