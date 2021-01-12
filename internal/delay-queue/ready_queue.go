package delayqueue

import (
	"fmt"

	"github.com/amazingchow/photon-dance-delay-queue/internal/redis"
)

/*
	key -> ready_queue_for_${topic}

	using LIST
*/

// 为了解决分布式并发竞争问题, 其他地方不能直接调用, 一律通过命令管道来统一分发命令
func (dq *DelayQueue) pushToReadyQueue(topic string, jobId string, debug bool) error {
	readyQ := fmt.Sprintf(DefaultReadyQueueNameFormatter, topic)
	_, err := redis.ExecCommand(dq.redisCli, debug, "RPUSH", readyQ, jobId)
	return err
}

// 为了解决分布式并发竞争问题, 其他地方不能直接调用, 一律通过命令管道来统一分发命令
func (dq *DelayQueue) blockPopFromReadyQueue(topics []string, timeout int, debug bool) (string, error) {
	var args []interface{}
	for _, topic := range topics {
		readyQ := fmt.Sprintf(DefaultReadyQueueNameFormatter, topic)
		args = append(args, readyQ)
	}
	args = append(args, timeout)

	v, err := redis.ExecCommand(dq.redisCli, debug, "BLPOP", args...)
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
