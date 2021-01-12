package delayqueue

import (
	"github.com/vmihailenco/msgpack"

	"github.com/amazingchow/photon-dance-delay-queue/internal/redis"
)

type Task struct {
	Topic string `json:"topic" msgpack:"1"` // 任务类型, 可以是具体的业务名称
	Id    string `json:"id" msgpack:"2"`    // 任务唯一标识, 用来检索/删除指定的任务
	Delay int64  `json:"delay" msgpack:"3"` // 任务需要延迟执行的时间, 单位: 秒
	TTR   int64  `json:"ttr" msgpack:"4"`   // 任务执行超时的时间, 单位: 秒
	Blob  string `json:"blob" msgpack:"5"`  // 任务内容, 供消费者做具体的业务处理, 以json格式存储
}

// 为了解决分布式并发竞争问题, 其他地方不能直接调用, 一律通过命令管道来统一分发命令
func (dq *DelayQueue) putTask(key string, task *Task) error {
	v, err := msgpack.Marshal(task)
	if err != nil {
		return err
	}
	_, err = redis.ExecCommand(dq.redisCli, false,  "SET", key, v)
	return err
}

// 为了解决分布式并发竞争问题, 其他地方不能直接调用, 一律通过命令管道来统一分发命令
func (dq *DelayQueue) getTask(key string) (*Task, error) {
	v, err := redis.ExecCommand(dq.redisCli, false,  "GET", key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}

	task := Task{}
	if err = msgpack.Unmarshal(v.([]byte), &task); err != nil {
		return nil, err
	}
	return &task, nil
}

// 为了解决分布式并发竞争问题, 其他地方不能直接调用, 一律通过命令管道来统一分发命令
func (dq *DelayQueue) delTask(key string) error {
	_, err := redis.ExecCommand(dq.redisCli, false,  "DEL", key)
	return err
}
