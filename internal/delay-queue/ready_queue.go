package delayqueue

import (
	"fmt"
)

func (dq *DelayQueue) pushToReadyQueue(topic string, jobId string) error {
	readyQ := fmt.Sprintf(DefaultReadyQueueNameFormatter, topic)
	_, err := dq.redisCli.ExecCommand("RPUSH", readyQ, jobId)
	return err
}

func (dq *DelayQueue) blockPopFromReadyQueue(topics []string, timeout int) (string, error) {
	var args []interface{}
	for _, topic := range topics {
		readyQ := fmt.Sprintf(DefaultReadyQueueNameFormatter, topic)
		args = append(args, readyQ)
	}
	args = append(args, timeout)

	v, err := dq.redisCli.ExecCommand("BLPOP", args...)
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
