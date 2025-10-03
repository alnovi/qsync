package utils

import (
	"context"

	"github.com/redis/go-redis/v9"
)

func IsCluster(client redis.UniversalClient) bool {
	if client == nil {
		return false
	}
	return client.Do(context.Background(), "CLUSTER", "NODES").Err() == nil
}
