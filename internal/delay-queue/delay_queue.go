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
	ctx      context.Context
	bucketCh <-chan string
	redisCli *redis.RedisPoolSingleton
}

func NewDelayQueue(ctx context.Context, cfg *conf.DelayQueue) *DelayQueue {
	dq := &DelayQueue{
		ctx:      ctx,
		bucketCh: spawnBuckets(ctx),
		redisCli: redis.GetOrCreateInstance(cfg.Backend),
	}
	go dq.poll(ctx)
	return dq
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
	if err := dq.putTask(task.Id, task); err != nil {
		log.Error().Err(err).Msgf("failed to add task <id: %s>", task.Id)
		return err
	}
	if err := dq.pushToBucket(<-dq.bucketCh, task.Delay, task.Id); err != nil {
		log.Error().Err(err).Msgf("failed to add task <id: %s> into bucket", task.Id)
		return err
	}
	return nil
}

func (dq *DelayQueue) Remove(taskId string) error {
	if err := dq.delTask(taskId); err != nil {
		log.Error().Err(err).Msgf("failed to remove task <id: %s>", taskId)
		return err
	}
	return nil
}

func (dq *DelayQueue) Get(taskId string) (*Task, error) {
	task, err := dq.getTask(taskId)
	if err != nil {
		log.Error().Err(err).Msgf("failed to get task <id: %s>", taskId)
		return task, err
	}
	// 任务不存在, 可能已被删除
	if task == nil {
		return nil, nil
	}
	return task, nil
}

func (dq *DelayQueue) startAllTimers() {
	for i := 0; i < DefaultBucketCnt; i++ {
		go dq.handleTimer(dq.ctx, fmt.Sprintf(DefaultBucketNameFormatter, i))
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
		bucketItem, err := dq.getFromBucket(bucket)
		if err != nil {
			log.Error().Err(err).Msgf("failed to scan bucket <name: %s>", bucket)
			return
		}
		if bucketItem == nil {
			return
		}
		// 延迟执行时间未到
		if bucketItem.TaskTimestamp > t.Unix() {
			return
		}

		// 延迟执行时间小于等于当前时间, 取出任务并放入ReadyQueue
		task, err := dq.getTask(bucketItem.TaskId)
		if err != nil {
			log.Error().Err(err).Msgf("failed to get task <id: %s>", bucketItem.TaskId)
			continue
		}
		// 任务不存在, 可能已被删除, 马上从bucket中删除
		if task == nil {
			if err = dq.delFromBucket(bucket, bucketItem.TaskId); err != nil {
				log.Error().Err(err).Msgf("failed to remove task <id: %s> from bucket", bucketItem.TaskId)
			}
			continue
		}
		// 再次确认任务延迟执行时间是否小于等于当前时间
		if task.Delay <= t.Unix() {
			if err = dq.pushToReadyQueue(task.Topic, task.Id); err != nil {
				log.Error().Err(err).Msgf("failed to add task <id: %s> into ready queue", task.Id)
				continue
			}
			if err = dq.delFromBucket(bucket, task.Id); err != nil {
				log.Error().Err(err).Msgf("failed to remove task <id: %s> from bucket", task.Id)
			}
		} else {
			if err = dq.delFromBucket(bucket, task.Id); err != nil {
				log.Error().Err(err).Msgf("failed to remove task <id: %s> from bucket", task.Id)
				continue
			}
			// 重新放入bucket中
			if err = dq.pushToBucket(<-dq.bucketCh, task.Delay, task.Id); err != nil {
				log.Error().Err(err).Msgf("failed to add task <id: %s> into bucket", task.Id)
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

				taskId, err := dq.blockPopFromReadyQueue(topics, 120)
				if err != nil {
					log.Error().Err(err).Msg("failed to pop from ready queue")
					continue
				}
				if taskId == "" {
					continue
				}

				task, err := dq.getTask(taskId)
				if err != nil {
					log.Error().Err(err).Msgf("failed to get task <id: %s>", taskId)
					continue
				}
				// 任务不存在, 可能已被删除
				if task == nil {
					continue
				}

				// TTR的设计目的是为了保证消息传输的可靠性
				// 任务执行完成后, 消费端需要调用finish接口去删除任务, 否则任务会重复投递, 消费端必须能处理同一任务多次投递的情形
				timestamp := time.Now().Unix() + task.TTR
				if err = dq.pushToBucket(<-dq.bucketCh, timestamp, task.Id); err != nil {
					log.Error().Err(err).Msgf("failed to add task <id: %s> into bucket", task.Id)
				}

				// TODO: publish ready task
			}
		}
	}
}
