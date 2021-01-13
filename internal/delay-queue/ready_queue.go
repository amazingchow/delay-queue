package delayqueue

import (
	"github.com/amazingchow/photon-dance-delay-queue/internal/redis"
)

/*
	key -> ready_queue_for_${topic}

	using LIST
*/

// 为了解决分布式并发竞争问题, 其他地方不能直接调用, 一律通过命令管道来统一分发命令
func (dq *DelayQueue) pushToReadyQueue(key string, jobId string, debug bool) error {
	_, err := redis.ExecCommand(dq.redisCli, debug, "RPUSH", key, jobId)
	return err
}

// 为了解决分布式并发竞争问题, 其他地方不能直接调用, 一律通过命令管道来统一分发命令
func (dq *DelayQueue) blockPopFromReadyQueue(key string, timeout int, debug bool) (string, error) {
	v, err := redis.ExecCommand(dq.redisCli, debug, "BLPOP", key, timeout)
	if err != nil {
		return "", err
	}
	if v == nil {
		return "", nil
	}

	vv := v.([]interface{})
	if len(vv) == 0 {
		return "", nil
	}
	return string(vv[1].([]byte)), nil
}
