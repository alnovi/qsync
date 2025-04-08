package qsync

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type broker struct {
	prefix string
	client redis.UniversalClient
}

func newBroker(prefix string, client redis.UniversalClient) *broker {
	prefix = strings.TrimSpace(prefix)
	prefix = strings.ToLower(prefix)
	prefix = prefix + ":qsync"
	prefix = strings.Trim(prefix, ":")
	return &broker{
		prefix: prefix,
		client: client,
	}
}

func (b *broker) Ping(ctx context.Context) error {
	return b.client.Ping(ctx).Err()
}

func (b *broker) Enqueue(ctx context.Context, queue string, msg *taskMessage) error {
	if time.Now().Before(msg.ProcessAt) {
		return b.enqueueScheduled(ctx, queue, msg)
	}
	return b.enqueuePending(ctx, queue, msg)
}

var cmdDequeuePending = redis.NewScript(`
local pendingKey = KEYS[1]
local taskPrefix = KEYS[2]

local taskKey = redis.call("RPOP", pendingKey)
if not taskKey then
	return redis.Nil
end

taskKey = taskPrefix .. ":" .. taskKey

local taskVal = redis.call("GET", taskKey)
if taskVal then
	redis.call("DEL", taskKey)
	return taskVal
end

return redis.Nil
`)

func (b *broker) Dequeue(ctx context.Context, queue string) (*taskMessage, error) {
	keys := []string{
		b.keyPending(queue),
		b.keyTasks(queue),
	}

	res, err := cmdDequeuePending.Run(ctx, b.client, keys).Result()
	if err != nil {
		return nil, err
	}

	task := new(taskMessage)

	err = json.Unmarshal([]byte(res.(string)), task)
	if err != nil {
		return nil, err
	}

	return task, nil
}

var cmdDequeueScheduled = redis.NewScript(`
local scheduledKey = KEYS[1]
local pendingKey = KEYS[2]

local currentTime = ARGV[1]

local ids = redis.call("ZRANGE", scheduledKey, "-inf", currentTime, "BYSCORE", "LIMIT", 0, 100)
for _, id in ipairs(ids) do
	redis.call("LPUSH", pendingKey, id)
	redis.call("ZREM", scheduledKey, id)
end

return 1
`)

func (b *broker) Scheduled(ctx context.Context, queue string) error {
	keys := []string{
		b.keyScheduled(queue),
		b.keyPending(queue),
	}

	args := []any{
		time.Now().UnixNano(),
	}

	return cmdDequeueScheduled.Run(ctx, b.client, keys, args).Err()
}

var cmdEnqueuePending = redis.NewScript(`
local taskKey = KEYS[1]
local taskVal = ARGV[1]

local queuesKey = KEYS[2]
local queuesVal = ARGV[2]

local pendingKey = KEYS[3]
local pendingVal = ARGV[3]

local ok = redis.call("SET", taskKey, taskVal, "NX")
if not ok then
	return 0
end

redis.call("SADD", queuesKey, queuesVal)
redis.call("LPUSH", pendingKey, pendingVal)

return 1
`)

func (b *broker) enqueuePending(ctx context.Context, queue string, msg *taskMessage) error {
	encoded, err := msg.Encode()
	if err != nil {
		return err
	}

	keys := []string{
		b.keyTask(queue, msg.Key()),
		b.keyQueues(),
		b.keyPending(queue),
	}

	args := []any{
		encoded,
		queue,
		msg.Key(),
	}

	res, err := cmdEnqueuePending.Run(ctx, b.client, keys, args...).Result()
	if err != nil {
		return err
	}

	if !b.cmdResBool(res) {
		return ErrTaskIsExists
	}

	return nil
}

var cmdEnqueueScheduled = redis.NewScript(`
local taskKey = KEYS[1]
local taskVal = ARGV[1]

local queuesKey = KEYS[2]
local queuesVal = ARGV[2]

local scheduledKey = KEYS[3]
local scheduledVal = ARGV[3]
local scheduledScore = ARGV[4]

local ok = redis.call("SET", taskKey, taskVal, "NX")
if not ok then
	return 0
end

redis.call("SADD", queuesKey, queuesVal)
redis.call("ZADD", scheduledKey, scheduledScore, scheduledVal)

return 1
`)

func (b *broker) enqueueScheduled(ctx context.Context, queue string, msg *taskMessage) error {
	encoded, err := msg.Encode()
	if err != nil {
		return err
	}

	keys := []string{
		b.keyTask(queue, msg.Key()),
		b.keyQueues(),
		b.keyScheduled(queue),
	}

	args := []any{
		encoded,
		queue,
		msg.Key(),
		msg.ProcessAt.UnixNano(),
	}

	res, err := cmdEnqueueScheduled.Run(ctx, b.client, keys, args...).Result()
	if err != nil {
		return err
	}

	if !b.cmdResBool(res) {
		return ErrTaskIsExists
	}

	return nil
}

func (b *broker) cmdResBool(res any) bool {
	if code, ok := res.(int64); ok {
		return code == 1
	} else {
		return false
	}
}

func (b *broker) keyQueues() string {
	return b.keyJoin("queues")
}

func (b *broker) keyTasks(queue string) string {
	return b.keyJoin(queue, "tasks")
}

func (b *broker) keyTask(queue string, key string) string {
	return b.keyJoin(queue, "tasks", key)
}

func (b *broker) keyPending(queue string) string {
	return b.keyJoin(queue, "pending")
}

func (b *broker) keyScheduled(queue string) string {
	return b.keyJoin(queue, "scheduled")
}

func (b *broker) keyJoin(elems ...string) string {
	return strings.Join(append([]string{b.prefix}, elems...), ":")
}
