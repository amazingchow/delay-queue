package delayqueue

import (
	"github.com/amazingchow/photon-dance-delay-queue/internal/redis"
)

/*
	key -> share the same default key "delay_queue_topic_set"

	using SET
*/

// 为了解决分布式并发竞争问题, 其他地方不能直接调用, 一律通过命令管道来统一分发命令
func (dq *DelayQueue) putTopic(key, topic string, debug bool) error {
	_, err := redis.ExecCommand(dq.redisCli, debug, "SADD", key, topic)
	return err
}

// 为了解决分布式并发竞争问题, 其他地方不能直接调用, 一律通过命令管道来统一分发命令
func (dq *DelayQueue) listTopic(key string, debug bool) ([]string, error) {
	v, err := redis.ExecCommand(dq.redisCli, debug, "SMEMBERS", key)
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
	topics := make([]string, len(vv))
	for i := 0; i < len(vv); i++ {
		topics[i] = string(vv[i].([]byte))
	}
	return topics, nil
}

// 为了解决分布式并发竞争问题, 其他地方不能直接调用, 一律通过命令管道来统一分发命令
func (dq *DelayQueue) hasTopic(key, topic string, debug bool) (bool, error) {
	v, err := redis.ExecCommand(dq.redisCli, debug, "SISMEMBER", key, topic)
	if err != nil {
		return false, err
	}
	if v == nil {
		return false, nil
	}
	return v.(int64) == 1, nil
}

// 为了解决分布式并发竞争问题, 其他地方不能直接调用, 一律通过命令管道来统一分发命令
func (dq *DelayQueue) delTopic(key, topic string, debug bool) error {
	_, err := redis.ExecCommand(dq.redisCli, debug, "SREM", key, topic)
	return err
}
