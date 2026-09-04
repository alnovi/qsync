package integration

import (
	"github.com/alnovi/qsync/v2"
)

func (s *TestSuite) TestMuxHandle() {
	testCases := []struct {
		name     string
		pattern  string
		handlers []*Handler
		expErr   string
	}{
		{
			name:    "Success",
			pattern: "test-handler",
			handlers: []*Handler{
				s.NewHandlerStub("test-handler"),
			},
			expErr: "",
		},
		{
			name:     "Not found",
			pattern:  "test-handler",
			handlers: []*Handler{},
			expErr:   "handler not found",
		},
		{
			name:    "Pattern is empty",
			pattern: "",
			handlers: []*Handler{
				s.NewHandlerStub("test-handler"),
			},
			expErr: "handler not found",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			mux := qsync.NewMux()

			for _, h := range tc.handlers {
				err := mux.Handler(h.NameTask(), h)
				s.Require().NoError(err, "failed to register handler")
			}

			_, err := mux.Handle(tc.pattern)
			if tc.expErr == "" {
				s.Require().NoError(err, "failed to get handle")
			} else {
				s.Require().Error(err, "expected error but got none")
				s.Require().ErrorContains(err, tc.expErr, "expected error but not Equal")
			}
		})
	}
}
