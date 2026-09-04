package integration

import "github.com/alnovi/qsync/v2"

func (s *TestSuite) TestMuxHandleFunc() {
	testCases := []struct {
		name     string
		handlers []*Handler
		expErr   string
	}{
		{
			name: "Success",
			handlers: []*Handler{
				s.NewHandlerStub("test-1"),
				s.NewHandlerStub("test-2"),
			},
			expErr: "",
		},
		{
			name: "Fail handler overlap",
			handlers: []*Handler{
				s.NewHandlerStub("test"),
				s.NewHandlerStub("test"),
			},
			expErr: "handler overlap",
		},
		{
			name: "Empty pattern",
			handlers: []*Handler{
				s.NewHandlerStub(""),
			},
			expErr: "pattern is empty",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			var err error

			mux := qsync.NewMux()
			for _, h := range tc.handlers {
				err = mux.HandleFunc(h.NameTask(), h.ProcessTask)
			}

			if tc.expErr == "" {
				s.Require().NoError(err, "mux.HandleFunc() return error")
			} else {
				s.Require().Error(err, "mux.HandleFunc() not return error")
				s.Require().ErrorContains(err, tc.expErr, "error not equal")
			}
		})
	}
}
