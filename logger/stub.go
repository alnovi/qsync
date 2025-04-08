package logger

type StubLogger struct{}

func NewStubLogger() *StubLogger {
	return new(StubLogger)
}

func (l *StubLogger) Debug(msg string, args ...any) {}

func (l *StubLogger) Info(msg string, args ...any) {}

func (l *StubLogger) Warn(msg string, args ...any) {}

func (l *StubLogger) Error(msg string, args ...any) {}
