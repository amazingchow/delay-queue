package delayqueue

import (
	"sync"

	"github.com/rs/zerolog/log"

	"github.com/amazingchow/photon-dance-delay-queue/internal/redis"
)

/*
	key -> ready_queue_for_${topic}

	using LIST
*/

type ReadyQueue struct {
	cond *sync.Cond
	len  int
}

func NewReadyQueue() *ReadyQueue {
	return &ReadyQueue{
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

func (rq *ReadyQueue) PushToReadyQueue(inst *redis.RedisConnPoolSingleton, key string, jobId string, debug bool) error {
	rq.cond.L.Lock()
	defer rq.cond.L.Unlock()

	_, err := redis.ExecCommand(inst, debug, "RPUSH", key, jobId)
	if err == nil {
		rq.len++
		rq.cond.Signal()
	}
	return err
}

func (rq *ReadyQueue) BlockPopFromReadyQueue(inst *redis.RedisConnPoolSingleton, key string, timeout int, debug bool) (string, error) {
	rq.cond.L.Lock()
	for rq.len == 0 {
		log.Debug().Msgf("ready queue is empty now, wait for task coming")
		rq.cond.Wait()
		log.Debug().Msgf("new task has been pushed, pop it now")
	}
	defer rq.cond.L.Unlock()

	v, err := redis.ExecCommand(inst, debug, "BLPOP", key, timeout)
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
	rq.len--
	return string(vv[1].([]byte)), nil
}
