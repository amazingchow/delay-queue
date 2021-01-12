package delayqueue

import (
	"context"

	"github.com/rs/zerolog/log"
)

type RedisRWRequest struct {
	RequestType RedisRequestType
	RequestOp   RedisRWRequestOp
	Inputs      []interface{}
	ResponseCh  chan *RedisRWResponse
}

type RedisRWResponse struct {
	Outputs []interface{}
	Err     error
}

type RedisRequestType int

const (
	TaskRequest       RedisRequestType = 1
	TopicRequest      RedisRequestType = 2
	BucketRequest     RedisRequestType = 3
	ReadyQueueRequest RedisRequestType = 4
)

type RedisRWRequestOp int

const (
	PutTaskRequest RedisRWRequestOp = 1
	GetTaskRequest RedisRWRequestOp = 2
	DelTaskRequest RedisRWRequestOp = 3

	PutTopicRequest  RedisRWRequestOp = 4
	ListTopicRequest RedisRWRequestOp = 5
	HasTopicRequest  RedisRWRequestOp = 6
	DelTopicRequest  RedisRWRequestOp = 7

	PushToBucketRequest  RedisRWRequestOp = 8
	GetFromBucketRequest RedisRWRequestOp = 9
	DelFromBucketRequest RedisRWRequestOp = 10

	PushToReadyQueueRequest       RedisRWRequestOp = 11
	BlockPopFromReadyQueueRequest RedisRWRequestOp = 12
)

// TODO: 设计更细粒度的并发控制, 仅针对单个key的操作做pipeline管理
func (dq *DelayQueue) sendRedisRWRequest(req *RedisRWRequest) {
	switch req.RequestType {
	case TaskRequest:
		{
			dq.taskRWChannel <- req
		}
	case TopicRequest:
		{
			dq.topicRWChannel <- req
		}
	case BucketRequest:
		{
			dq.bucketRWChannel <- req
		}
	case ReadyQueueRequest:
		{
			dq.readyQueueRWChannel <- req
		}
	default:
		{
			log.Error().Msgf("invalid RedisRequestType")
		}
	}
}

func (dq *DelayQueue) handleTaskRWRequest(ctx context.Context) {
REDIS_RW_LOOP:
	for {
		select {
		case <-ctx.Done():
			{
				break REDIS_RW_LOOP
			}
		case req := <-dq.taskRWChannel:
			{
				if req.RequestOp == PutTaskRequest {
					err := dq.putTask(req.Inputs[0].(string), req.Inputs[1].(*Task))
					req.ResponseCh <- &RedisRWResponse{
						Err: err,
					}
				} else if req.RequestOp == GetTaskRequest {
					v, err := dq.getTask(req.Inputs[0].(string))
					if err != nil {
						req.ResponseCh <- &RedisRWResponse{
							Err: err,
						}
					} else {
						req.ResponseCh <- &RedisRWResponse{
							Outputs: []interface{}{v},
							Err:     nil,
						}
					}
				} else if req.RequestOp == DelTaskRequest {
					err := dq.delTask(req.Inputs[0].(string))
					req.ResponseCh <- &RedisRWResponse{
						Err: err,
					}
				}
			}
		}
	}
}

func (dq *DelayQueue) handleTopicRWRequest(ctx context.Context) {
REDIS_RW_LOOP:
	for {
		select {
		case <-ctx.Done():
			{
				break REDIS_RW_LOOP
			}
		case req := <-dq.topicRWChannel:
			{
				if req.RequestOp == PutTopicRequest {
					err := dq.putTopic(req.Inputs[0].(string), req.Inputs[1].(string))
					req.ResponseCh <- &RedisRWResponse{
						Err: err,
					}
				} else if req.RequestOp == ListTopicRequest {
					v, err := dq.listTopic(req.Inputs[0].(string))
					if err != nil {
						req.ResponseCh <- &RedisRWResponse{
							Err: err,
						}
					} else {
						req.ResponseCh <- &RedisRWResponse{
							Outputs: []interface{}{v},
							Err:     nil,
						}
					}
				} else if req.RequestOp == HasTopicRequest {
					v, err := dq.hasTopic(req.Inputs[0].(string), req.Inputs[1].(string))
					if err != nil {
						req.ResponseCh <- &RedisRWResponse{
							Err: err,
						}
					} else {
						req.ResponseCh <- &RedisRWResponse{
							Outputs: []interface{}{v},
							Err:     nil,
						}
					}
				} else if req.RequestOp == DelTopicRequest {
					err := dq.delTopic(req.Inputs[0].(string), req.Inputs[1].(string))
					req.ResponseCh <- &RedisRWResponse{
						Err: err,
					}
				}
			}
		}
	}
}

func (dq *DelayQueue) handleBucketRWRequest(ctx context.Context) {
REDIS_RW_LOOP:
	for {
		select {
		case <-ctx.Done():
			{
				break REDIS_RW_LOOP
			}
		case req := <-dq.bucketRWChannel:
			{
				if req.RequestOp == PushToBucketRequest {
					err := dq.pushToBucket(req.Inputs[0].(string), req.Inputs[1].(int64), req.Inputs[2].(string))
					req.ResponseCh <- &RedisRWResponse{
						Err: err,
					}
				} else if req.RequestOp == GetFromBucketRequest {
					v, err := dq.getFromBucket(req.Inputs[0].(string))
					if err != nil {
						req.ResponseCh <- &RedisRWResponse{
							Err: err,
						}
					} else {
						req.ResponseCh <- &RedisRWResponse{
							Outputs: []interface{}{v},
							Err:     nil,
						}
					}
				} else if req.RequestOp == DelFromBucketRequest {
					err := dq.delFromBucket(req.Inputs[0].(string), req.Inputs[1].(string))
					req.ResponseCh <- &RedisRWResponse{
						Err: err,
					}
				}
			}
		}
	}
}

func (dq *DelayQueue) handleReadyQueueRWRequest(ctx context.Context) {
REDIS_RW_LOOP:
	for {
		select {
		case <-ctx.Done():
			{
				break REDIS_RW_LOOP
			}
		case req := <-dq.readyQueueRWChannel:
			{
				if req.RequestOp == PushToReadyQueueRequest {
					err := dq.pushToReadyQueue(req.Inputs[0].(string), req.Inputs[1].(string))
					req.ResponseCh <- &RedisRWResponse{
						Err: err,
					}
				} else if req.RequestOp == BlockPopFromReadyQueueRequest {
					v, err := dq.blockPopFromReadyQueue(req.Inputs[0].([]string), req.Inputs[1].(int))
					if err != nil {
						req.ResponseCh <- &RedisRWResponse{
							Err: err,
						}
					} else {
						req.ResponseCh <- &RedisRWResponse{
							Outputs: []interface{}{v},
							Err:     nil,
						}
					}
				}
			}
		}
	}
}
