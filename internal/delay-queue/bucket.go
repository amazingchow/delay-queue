package delayqueue

import (
	"strconv"

	"github.com/amazingchow/photon-dance-delay-queue/internal/redis"
)

type BucketItem struct {
	TaskId        string
	TaskTimestamp int64
}

// 为了解决分布式并发竞争问题, 其他地方不能直接调用, 一律通过命令管道来统一分发命令
func (dq *DelayQueue) pushToBucket(key string, timestamp int64, id string) error {
	_, err := redis.ExecCommand(dq.redisCli, false,  "ZADD", key, timestamp, id)
	return err
}

// 为了解决分布式并发竞争问题, 其他地方不能直接调用, 一律通过命令管道来统一分发命令
func (dq *DelayQueue) getFromBucket(key string) (*BucketItem, error) {
	v, err := redis.ExecCommand(dq.redisCli, false,  "ZRANGE", key, 0, 0, "WITHSCORES")
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	vv := v.([]interface{})
	if len(vv) == 0 {
		return nil, nil
	}

	item := BucketItem{}
	item.TaskId = string(vv[0].([]byte))
	timestampStr := string(vv[1].([]byte))
	item.TaskTimestamp, _ = strconv.ParseInt(timestampStr, 10, 64)
	return &item, nil
}

// 为了解决分布式并发竞争问题, 其他地方不能直接调用, 一律通过命令管道来统一分发命令
func (dq *DelayQueue) delFromBucket(key string, id string) error {
	_, err := redis.ExecCommand(dq.redisCli, false,  "ZREM", key, id)
	return err
}
