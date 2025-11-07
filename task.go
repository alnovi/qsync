package qsync

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/alnovi/qsync/utils"
)

const (
	taskIdCost     = 6
	taskDelay      = 0
	taskRetry      = 3
	taskRetryDelay = 0
	taskMaxRetry   = 5
)

var (
	ErrTaskIsNil       = errors.New("task is nil")
	ErrTaskTypeIsEmpty = errors.New("task type is empty")
	ErrTaskIsExists    = errors.New("task is exists")
	ErrTaskFailEncode  = errors.New("fail encode task")
	ErrTaskIsDeadline  = errors.New("task is deadline")
)

type Task struct {
	id         string
	typename   string
	payload    []byte
	delay      time.Duration
	retry      int
	retryDelay time.Duration
	deadline   time.Time
	processAt  time.Time
}

func NewTask(typename string, payload []byte, opts ...TaskOption) *Task {
	task := &Task{
		id:         "",
		typename:   strings.TrimSpace(typename),
		payload:    payload,
		delay:      taskDelay,
		retry:      taskRetry,
		retryDelay: taskRetryDelay,
		deadline:   time.Time{},
		processAt:  time.Time{},
	}

	for _, opt := range opts {
		opt(task)
	}

	return task
}

type TaskOption func(*Task)

func WithId(id string) TaskOption {
	return func(task *Task) {
		task.id = strings.TrimSpace(id)
	}
}

func WithDelay(delay time.Duration) TaskOption {
	return func(task *Task) {
		task.delay = delay
	}
}

func WithRetry(retry int) TaskOption {
	return func(task *Task) {
		task.retry = retry
	}
}

func WithRetryDelay(delay time.Duration) TaskOption {
	return func(task *Task) {
		task.retryDelay = delay
	}
}

func WithDeadline(deadline time.Time) TaskOption {
	return func(task *Task) {
		task.deadline = deadline
	}
}

func WithProcessAt(processAt time.Time) TaskOption {
	return func(task *Task) {
		task.processAt = processAt
	}
}

type TaskInfo struct {
	Id      string
	Type    string
	Payload []byte
	Retry   int
	Retried int
}

func newTaskInfo(msg *taskMessage) *TaskInfo {
	return &TaskInfo{
		Id:      msg.Id,
		Type:    msg.Type,
		Payload: bytes.Clone(msg.Payload),
		Retry:   msg.Retry,
		Retried: msg.Retried,
	}
}

type taskMessage struct {
	Id         string        `json:"id"`
	Type       string        `json:"type"`
	Payload    []byte        `json:"payload"`
	Retry      int           `json:"retry"`
	Retried    int           `json:"retried"`
	RetryDelay time.Duration `json:"retry_delay"`
	Deadline   time.Time     `json:"deadline"`
	ProcessAt  time.Time     `json:"-"`
}

func newTaskMessage(task *Task) (*taskMessage, error) {
	if task == nil {
		return nil, ErrTaskIsNil
	}

	msg := &taskMessage{
		Id:         task.id,
		Type:       task.typename,
		Payload:    task.payload,
		Retry:      task.retry,
		RetryDelay: task.retryDelay,
		Deadline:   time.Time{},
		ProcessAt:  time.Time{},
	}

	if msg.Id == "" {
		msg.Id = utils.RandBase62(taskIdCost)
	}

	if msg.Type == "" {
		return nil, ErrTaskTypeIsEmpty
	}

	if msg.Retry > taskMaxRetry {
		msg.Retry = taskMaxRetry
	}

	if !task.deadline.IsZero() && task.deadline.After(time.Now()) {
		msg.Deadline = task.deadline
	}

	if task.delay.Seconds() > 0 {
		msg.ProcessAt = time.Now().Add(task.delay)
	}

	if !task.processAt.IsZero() && task.processAt.After(time.Now()) {
		msg.ProcessAt = task.processAt
	}

	return msg, nil
}

func (t *taskMessage) Key() string {
	return t.Type + "-" + t.Id
}

func (t *taskMessage) Encode() ([]byte, error) {
	data, err := json.Marshal(t)
	if err != nil {
		return nil, fmt.Errorf("%w [task-key=%s]: %w", ErrTaskFailEncode, t.Key(), err)
	}
	return data, nil
}

func (t *taskMessage) IsDeadline() error {
	if !t.Deadline.IsZero() && time.Now().After(t.Deadline) {
		return ErrTaskIsDeadline
	}
	return nil
}
