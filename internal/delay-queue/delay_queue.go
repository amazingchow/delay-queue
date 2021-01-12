package delayqueue

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	pb "github.com/amazingchow/photon-dance-delay-queue/api"
	conf "github.com/amazingchow/photon-dance-delay-queue/internal/config"
	"github.com/amazingchow/photon-dance-delay-queue/internal/kafka"
	"github.com/amazingchow/photon-dance-delay-queue/internal/redis"
)

type DelayQueue struct {
	ctx    context.Context
	cancel context.CancelFunc

	taskRWController *TaskRWController
	topicRWChannel   chan *RedisRWRequest
	bucketRWChannel  chan *RedisRWRequest
	readyQRWChannel  chan *RedisRWRequest

	bucketCh <-chan string
	redisCli *redis.RedisConnPoolSingleton
	producer *kafka.Producer
}

func NewDelayQueue(cfg *conf.DelayQueueService) *DelayQueue {
	ctx, cancel := context.WithCancel(context.Background())
	dq := &DelayQueue{
		ctx:    ctx,
		cancel: cancel,

		taskRWController: NewTaskRWController(),
		topicRWChannel:   make(chan *RedisRWRequest, 1024),
		bucketRWChannel:  make(chan *RedisRWRequest, 1024),
		readyQRWChannel:  make(chan *RedisRWRequest, 1024),

		bucketCh: spawnBuckets(ctx),
		redisCli: redis.GetOrCreateInstance(cfg.RedisService),
		producer: kafka.NewProducer(cfg.KafkaService),
	}
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
	dq.producer.Close()
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

// DelayQueue的Push/PushTopic/RemoveTopic按照原来的处理流程, 可能会受主循环影响而被阻塞
// 因此作乐观处理, 不等待处理结果直接返回, 由调用方确认是否操作成功

func (dq *DelayQueue) Push(task *Task) error {
	/* start to add task */
	err := dq.taskRWController.PutTask(dq.redisCli, task.Id, task, false)
	if err != nil {
		log.Error().Err(err).Msgf("failed to add task <id: %s>", task.Id)
		return err
	}

	/* start to push task into bucket */
	resp := make(chan *RedisRWResponse, 1)
	req := &RedisRWRequest{
		RequestType: BucketRequest,
		RequestOp:   PushToBucketRequest,
		Inputs:      []interface{}{<-dq.bucketCh, task.Delay, task.Id, false},
		ResponseCh:  resp,
	}
	dq.sendRedisRWRequest(req)
	log.Debug().Msgf("add a new task <%s>", task.Id)
	return nil
}

func (dq *DelayQueue) Remove(taskId string) error {
	/* start to delete task */
	err := dq.taskRWController.DelTask(dq.redisCli, taskId, false)
	if err != nil {
		log.Error().Err(err).Msgf("failed to remove task <id: %s>", taskId)
		return err
	}
	log.Debug().Msgf("delete a task <%s>", taskId)
	return nil
}

func (dq *DelayQueue) Get(taskId string) (*Task, error) {
	/* start to get task */
	task, err := dq.taskRWController.GetTask(dq.redisCli, taskId, false)
	if err != nil {
		log.Error().Err(err).Msgf("failed to get task <id: %s>", taskId)
		return nil, err
	}
	// 任务不存在, 可能已被删除
	if task == nil {
		return nil, nil
	}
	return task, nil
}

func (dq *DelayQueue) PushTopic(topic string) error {
	/* start to add topic */
	resp := make(chan *RedisRWResponse, 1)
	req := &RedisRWRequest{
		RequestType: TopicRequest,
		RequestOp:   PutTopicRequest,
		Inputs:      []interface{}{DefaultTopicSetName, topic, false},
		ResponseCh:  resp,
	}
	dq.sendRedisRWRequest(req)
	return nil
}

func (dq *DelayQueue) RemoveTopic(topic string) error {
	/* start to delete topic */
	resp := make(chan *RedisRWResponse, 1)
	req := &RedisRWRequest{
		RequestType: TopicRequest,
		RequestOp:   DelTopicRequest,
		Inputs:      []interface{}{DefaultTopicSetName, topic, false},
		ResponseCh:  resp,
	}
	dq.sendRedisRWRequest(req)
	return nil
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
		/* start to get task from bucket */
		resp := make(chan *RedisRWResponse)
		req := &RedisRWRequest{
			RequestType: BucketRequest,
			RequestOp:   GetOneFromBucketRequest,
			Inputs:      []interface{}{bucket, false},
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
		/* start to get task */
		task, err := dq.taskRWController.GetTask(dq.redisCli, bucketItem.TaskId, false)
		if err != nil {
			log.Error().Err(err).Msgf("failed to get task <id: %s>", bucketItem.TaskId)
			continue
		}
		// 任务不存在, 可能已被删除, 马上从bucket中删除
		if task == nil {
			/* start to delete task from bucket */
			resp := make(chan *RedisRWResponse)
			req := &RedisRWRequest{
				RequestType: BucketRequest,
				RequestOp:   DelFromBucketRequest,
				Inputs:      []interface{}{bucket, bucketItem.TaskId, false},
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
			/* start to push task into ready queue */
			resp := make(chan *RedisRWResponse)
			req := &RedisRWRequest{
				RequestType: ReadyQueueRequest,
				RequestOp:   PushToReadyQueueRequest,
				Inputs:      []interface{}{task.Topic, task.Id, false},
				ResponseCh:  resp,
			}
			dq.sendRedisRWRequest(req)
			outs := <-resp
			if outs.Err != nil {
				log.Error().Err(outs.Err).Msgf("failed to add task <id: %s> into ready queue", task.Id)
				continue
			}

			/* start to delete task from bucket */
			resp = make(chan *RedisRWResponse)
			req = &RedisRWRequest{
				RequestType: BucketRequest,
				RequestOp:   DelFromBucketRequest,
				Inputs:      []interface{}{bucket, task.Id, false},
				ResponseCh:  resp,
			}
			dq.sendRedisRWRequest(req)
			outs = <-resp
			if outs.Err != nil {
				log.Error().Err(outs.Err).Msgf("failed to remove task <id: %s> from bucket", task.Id)
			}
		} else {
			/* start to delete task from bucket */
			resp := make(chan *RedisRWResponse)
			req := &RedisRWRequest{
				RequestType: BucketRequest,
				RequestOp:   DelFromBucketRequest,
				Inputs:      []interface{}{bucket, task.Id, false},
				ResponseCh:  resp,
			}
			dq.sendRedisRWRequest(req)
			outs := <-resp
			if outs.Err != nil {
				log.Error().Err(outs.Err).Msgf("failed to remove task <id: %s> from bucket", task.Id)
				continue
			}

			// 重新放入bucket中
			/* start to push task into bucket */
			resp = make(chan *RedisRWResponse)
			req = &RedisRWRequest{
				RequestType: BucketRequest,
				RequestOp:   PushToBucketRequest,
				Inputs:      []interface{}{<-dq.bucketCh, task.Delay, task.Id, false},
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
				/* start to get all subscribed topics */
				resp := make(chan *RedisRWResponse)
				req := &RedisRWRequest{
					RequestType: TopicRequest,
					RequestOp:   ListTopicRequest,
					Inputs:      []interface{}{DefaultTopicSetName, false},
					ResponseCh:  resp,
				}
				dq.sendRedisRWRequest(req)
				outs := <-resp
				if outs.Err != nil {
					log.Error().Err(outs.Err).Msg("failed to list topic")
					continue
				}
				topics := outs.Outputs[0].([]string)
				if len(topics) == 0 {
					continue
				}
				log.Debug().Msgf("all subscribed topics: %v", topics)

				/* start to pop task from ready queue */
				// TODO: 在无任务和有任务两种状态之间切换会带来额外的延时, 可能会影响具体的业务
				resp = make(chan *RedisRWResponse)
				req = &RedisRWRequest{
					RequestType: ReadyQueueRequest,
					RequestOp:   BlockPopFromReadyQueueRequest,
					Inputs:      []interface{}{topics, DefaultBlockPopFromReadyQueueTimeout, false},
					ResponseCh:  resp,
				}
				dq.sendRedisRWRequest(req)
				outs = <-resp
				if outs.Err != nil {
					log.Error().Err(outs.Err).Msg("failed to pop from ready queue")
					continue
				}
				taskId := outs.Outputs[0].(string)
				if taskId == "" {
					continue
				}
				log.Debug().Msgf("get ready task <%s>", taskId)

				/* start to get task */
				task, err := dq.taskRWController.GetTask(dq.redisCli, taskId, false)
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
				/* start to push task into bucket */
				resp = make(chan *RedisRWResponse)
				req = &RedisRWRequest{
					RequestType: BucketRequest,
					RequestOp:   PushToBucketRequest,
					Inputs:      []interface{}{<-dq.bucketCh, timestamp, task.Id, false},
					ResponseCh:  resp,
				}
				dq.sendRedisRWRequest(req)
				outs = <-resp
				if outs.Err != nil {
					log.Error().Err(outs.Err).Msgf("failed to add task <id: %s> into bucket", task.Id)
				}

				// publish ready task to kafka
				msg, _ := proto.Marshal(&pb.Task{
					Id:            task.Id,
					AttachedTopic: task.Topic,
					Payload:       task.Blob,
				})
				if err := dq.producer.Publish(task.Topic, msg); err != nil {
					log.Error().Err(err).Msgf("failed to publish ready task <id: %s>", task.Id)
				}
				log.Debug().Msgf("send ready task <%s> to kafka", taskId)
			}
		}
	}
}
