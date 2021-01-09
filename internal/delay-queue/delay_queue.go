package delayqueue

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	conf "github.com/amazingchow/photon-dance-delay-queue/internal/config"
	"github.com/amazingchow/photon-dance-delay-queue/internal/redis"
)

type DelayQueue struct {
	ctx    context.Context
	cancel context.CancelFunc

	taskRWChannel       chan *RedisRWRequest
	topicRWChannel      chan *RedisRWRequest
	bucketRWChannel     chan *RedisRWRequest
	readyQueueRWChannel chan *RedisRWRequest

	bucketCh <-chan string
	redisCli *redis.RedisPoolSingleton
}

func NewDelayQueue(cfg *conf.DelayQueue) *DelayQueue {
	ctx, cancel := context.WithCancel(context.Background())
	dq := &DelayQueue{
		ctx:    ctx,
		cancel: cancel,

		taskRWChannel:       make(chan *RedisRWRequest, 1024),
		topicRWChannel:      make(chan *RedisRWRequest, 1024),
		bucketRWChannel:     make(chan *RedisRWRequest, 1024),
		readyQueueRWChannel: make(chan *RedisRWRequest, 1024),

		bucketCh: spawnBuckets(ctx),
		redisCli: redis.GetOrCreateInstance(cfg.Backend),
	}
	go dq.handleTaskRWRequest(ctx)
	go dq.handleTopicRWRequest(ctx)
	go dq.handleBucketRWRequest(ctx)
	go dq.handleReadyQueueRWRequest(ctx)
	go dq.startAllTimers(ctx)
	go dq.poll(ctx)
	return dq
}

func (dq *DelayQueue) Close() {
	redis.ReleaseInstance()
	if dq.cancel != nil {
		dq.cancel()
	}
}

func spawnBuckets(ctx context.Context) <-chan string {
	ch := make(chan string)
	go func(ctx context.Context, ch chan string) {
		i := 1
	SPAWN_LOOP:
		for {
			select {
			case <-ctx.Done():
				{
					break SPAWN_LOOP
				}
			default:
				{
					ch <- fmt.Sprintf(DefaultBucketNameFormatter, i)
					if i >= DefaultBucketCnt {
						i = 1
					} else {
						i++
					}
				}
			}
		}
	}(ctx, ch)
	return ch
}

func (dq *DelayQueue) Push(task *Task) error {
	/* start put task */
	resp := make(chan *RedisRWResponse)
	req := &RedisRWRequest{
		RequestType: TaskRequest,
		RequestOp:   PutTaskRequest,
		Inputs:      []interface{}{task.Id, task},
		ResponseCh:  resp,
	}
	dq.sendRedisRWRequest(req)
	outs := <-resp
	if outs.Err != nil {
		log.Error().Err(outs.Err).Msgf("failed to add task <id: %s>", task.Id)
		return outs.Err
	}

	/* start push task into bucket */
	resp = make(chan *RedisRWResponse)
	req = &RedisRWRequest{
		RequestType: BucketRequest,
		RequestOp:   PushToBucketRequest,
		Inputs:      []interface{}{<-dq.bucketCh, task.Delay, task.Id},
		ResponseCh:  resp,
	}
	dq.sendRedisRWRequest(req)
	outs = <-resp
	if outs.Err != nil {
		log.Error().Err(outs.Err).Msgf("failed to add task <id: %s> into bucket", task.Id)
		return outs.Err
	}
	return nil
}

func (dq *DelayQueue) Remove(taskId string) error {
	/* start delete task */
	resp := make(chan *RedisRWResponse)
	req := &RedisRWRequest{
		RequestType: TaskRequest,
		RequestOp:   DelTaskRequest,
		Inputs:      []interface{}{taskId},
		ResponseCh:  resp,
	}
	dq.sendRedisRWRequest(req)
	outs := <-resp
	if outs.Err != nil {
		log.Error().Err(outs.Err).Msgf("failed to remove task <id: %s>", taskId)
		return outs.Err
	}
	return nil
}

func (dq *DelayQueue) Get(taskId string) (*Task, error) {
	/* start delete task */
	resp := make(chan *RedisRWResponse)
	req := &RedisRWRequest{
		RequestType: TaskRequest,
		RequestOp:   GetTaskRequest,
		Inputs:      []interface{}{taskId},
		ResponseCh:  resp,
	}
	dq.sendRedisRWRequest(req)
	outs := <-resp
	if outs.Err != nil {
		log.Error().Err(outs.Err).Msgf("failed to get task <id: %s>", taskId)
		return nil, outs.Err
	}
	task := outs.Outputs[0].(*Task)
	// 任务不存在, 可能已被删除
	if task == nil {
		return nil, nil
	}
	return task, nil
}

func (dq *DelayQueue) startAllTimers(ctx context.Context) {
	for i := 0; i < DefaultBucketCnt; i++ {
		go dq.handleTimer(ctx, fmt.Sprintf(DefaultBucketNameFormatter, i))
	}
}

func (dq *DelayQueue) handleTimer(ctx context.Context, bucket string) {
	timer := time.NewTicker(1 * time.Second)
TICK_LOOP:
	for {
		select {
		case <-ctx.Done():
			{
				break TICK_LOOP
			}
		case t := <-timer.C:
			{
				dq.timerHandler(t, bucket)
			}
		}
	}
}

func (dq *DelayQueue) timerHandler(t time.Time, bucket string) {
	for {
		/* start get task from bucket */
		resp := make(chan *RedisRWResponse)
		req := &RedisRWRequest{
			RequestType: BucketRequest,
			RequestOp:   GetFromBucketRequest,
			Inputs:      []interface{}{bucket},
			ResponseCh:  resp,
		}
		dq.sendRedisRWRequest(req)
		outs := <-resp
		if outs.Err != nil {
			log.Error().Err(outs.Err).Msgf("failed to scan bucket <name: %s>", bucket)
			return
		}
		bucketItem := outs.Outputs[0].(*BucketItem)
		if bucketItem == nil {
			return
		}
		// 延迟执行时间未到
		if bucketItem.TaskTimestamp > t.Unix() {
			return
		}

		// 延迟执行时间小于等于当前时间, 取出任务并放入ReadyQueue
		/* start get task */
		resp = make(chan *RedisRWResponse)
		req = &RedisRWRequest{
			RequestType: TaskRequest,
			RequestOp:   GetTaskRequest,
			Inputs:      []interface{}{bucketItem.TaskId},
			ResponseCh:  resp,
		}
		dq.sendRedisRWRequest(req)
		outs = <-resp
		if outs.Err != nil {
			log.Error().Err(outs.Err).Msgf("failed to get task <id: %s>", bucketItem.TaskId)
			continue
		}
		task := outs.Outputs[0].(*Task)
		// 任务不存在, 可能已被删除, 马上从bucket中删除
		if task == nil {
			/* start delete task from task */
			resp := make(chan *RedisRWResponse)
			req := &RedisRWRequest{
				RequestType: BucketRequest,
				RequestOp:   DelFromBucketRequest,
				Inputs:      []interface{}{bucket, bucketItem.TaskId},
				ResponseCh:  resp,
			}
			dq.sendRedisRWRequest(req)
			outs := <-resp
			if outs.Err != nil {
				log.Error().Err(outs.Err).Msgf("failed to remove task <id: %s> from bucket", bucketItem.TaskId)
			}
			continue
		}
		// 再次确认任务延迟执行时间是否小于等于当前时间
		if task.Delay <= t.Unix() {
			/* start push task into ready queue */
			resp := make(chan *RedisRWResponse)
			req := &RedisRWRequest{
				RequestType: ReadyQueueRequest,
				RequestOp:   PushToReadyQueueRequest,
				Inputs:      []interface{}{task.Topic, task.Id},
				ResponseCh:  resp,
			}
			dq.sendRedisRWRequest(req)
			outs := <-resp
			if outs.Err != nil {
				log.Error().Err(outs.Err).Msgf("failed to add task <id: %s> into ready queue", task.Id)
				continue
			}

			/* start delete task from task */
			resp = make(chan *RedisRWResponse)
			req = &RedisRWRequest{
				RequestType: BucketRequest,
				RequestOp:   DelFromBucketRequest,
				Inputs:      []interface{}{bucket, task.Id},
				ResponseCh:  resp,
			}
			dq.sendRedisRWRequest(req)
			outs = <-resp
			if outs.Err != nil {
				log.Error().Err(outs.Err).Msgf("failed to remove task <id: %s> from bucket", task.Id)
			}
		} else {
			/* start delete task from task */
			resp := make(chan *RedisRWResponse)
			req := &RedisRWRequest{
				RequestType: BucketRequest,
				RequestOp:   DelFromBucketRequest,
				Inputs:      []interface{}{bucket, task.Id},
				ResponseCh:  resp,
			}
			dq.sendRedisRWRequest(req)
			outs := <-resp
			if outs.Err != nil {
				log.Error().Err(outs.Err).Msgf("failed to remove task <id: %s> from bucket", task.Id)
				continue
			}

			// 重新放入bucket中
			/* start push task into bucket */
			resp = make(chan *RedisRWResponse)
			req = &RedisRWRequest{
				RequestType: BucketRequest,
				RequestOp:   PushToBucketRequest,
				Inputs:      []interface{}{<-dq.bucketCh, task.Delay, task.Id},
				ResponseCh:  resp,
			}
			dq.sendRedisRWRequest(req)
			outs = <-resp
			if outs.Err != nil {
				log.Error().Err(outs.Err).Msgf("failed to add task <id: %s> into bucket", task.Id)
			}
		}
	}
}

func (dq *DelayQueue) poll(ctx context.Context) {
POLL_LOOP:
	for {
		select {
		case <-ctx.Done():
			{
				break POLL_LOOP
			}
		default:
			{
				// TODO: get all subscribed topics
				topics := make([]string, 0)

				/* start pop task from ready queue */
				resp := make(chan *RedisRWResponse)
				req := &RedisRWRequest{
					RequestType: ReadyQueueRequest,
					RequestOp:   BlockPopFromReadyQueueRequest,
					Inputs:      []interface{}{topics, 120},
					ResponseCh:  resp,
				}
				dq.sendRedisRWRequest(req)
				outs := <-resp
				if outs.Err != nil {
					log.Error().Err(outs.Err).Msg("failed to pop from ready queue")
					continue
				}
				taskId := outs.Outputs[0].(string)
				if taskId == "" {
					continue
				}

				/* start get task */
				resp = make(chan *RedisRWResponse)
				req = &RedisRWRequest{
					RequestType: TaskRequest,
					RequestOp:   GetTaskRequest,
					Inputs:      []interface{}{taskId},
					ResponseCh:  resp,
				}
				dq.sendRedisRWRequest(req)
				outs = <-resp
				if outs.Err != nil {
					log.Error().Err(outs.Err).Msgf("failed to get task <id: %s>", taskId)
					continue
				}
				task := outs.Outputs[0].(*Task)
				// 任务不存在, 可能已被删除
				if task == nil {
					continue
				}

				// TTR的设计目的是为了保证消息传输的可靠性
				// 任务执行完成后, 消费端需要调用finish接口去删除任务, 否则任务会重复投递, 消费端必须能处理同一任务多次投递的情形
				timestamp := time.Now().Unix() + task.TTR
				/* start push task into bucket */
				resp = make(chan *RedisRWResponse)
				req = &RedisRWRequest{
					RequestType: BucketRequest,
					RequestOp:   PushToBucketRequest,
					Inputs:      []interface{}{<-dq.bucketCh, timestamp, task.Id},
					ResponseCh:  resp,
				}
				dq.sendRedisRWRequest(req)
				outs = <-resp
				if outs.Err != nil {
					log.Error().Err(outs.Err).Msgf("failed to add task <id: %s> into bucket", task.Id)
				}

				// TODO: publish ready task
			}
		}
	}
}
