package delayqueue

// 为了解决分布式并发竞争问题, 其他地方不能直接调用, 一律通过命令管道来统一分发命令
func (dq *DelayQueue) putTopic(key, topic string) error {
	_, err := dq.redisCli.ExecCommand("SADD", key, topic)
	return err
}

// 为了解决分布式并发竞争问题, 其他地方不能直接调用, 一律通过命令管道来统一分发命令
func (dq *DelayQueue) listTopic(key string) ([]string, error) {
	v, err := dq.redisCli.ExecCommand("SMEMBERS", key)
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
func (dq *DelayQueue) hasTopic(key, topic string) (bool, error) {
	v, err := dq.redisCli.ExecCommand("SISMEMBER", key, topic)
	if err != nil {
		return false, err
	}
	if v == nil {
		return false, nil
	}
	return v.(int64) == 1, nil
}

// 为了解决分布式并发竞争问题, 其他地方不能直接调用, 一律通过命令管道来统一分发命令
func (dq *DelayQueue) delTopic(key, topic string) error {
	_, err := dq.redisCli.ExecCommand("SREM", key, topic)
	return err
}
