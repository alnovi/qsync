package qsync

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrHandlerOverlap  = errors.New("handler overlap")
	ErrHandlerNotFound = errors.New("handler not found")
)

type Handler interface {
	ProcessTask(ctx context.Context, task *TaskInfo) error
}

type HandleFunc func(ctx context.Context, task *TaskInfo) error

type Mux struct {
	handlers map[string]HandleFunc
	mu       sync.RWMutex
}

func NewMux() *Mux {
	return &Mux{handlers: make(map[string]HandleFunc)}
}

func (m *Mux) Handler(pattern string, handler Handler) error {
	return m.HandleFunc(pattern, handler.ProcessTask)
}

func (m *Mux) HandleFunc(pattern string, handler HandleFunc) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.handlers[pattern]; ok {
		return ErrHandlerOverlap
	}
	m.handlers[pattern] = handler
	return nil
}

func (m *Mux) Handle(pattern string) (HandleFunc, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	handler, ok := m.handlers[pattern]
	if !ok {
		return nil, ErrHandlerNotFound
	}
	return handler, nil
}
