package main

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/amazingchow/photon-dance-delay-queue/api"
	conf "github.com/amazingchow/photon-dance-delay-queue/internal/config"
	delayqueue "github.com/amazingchow/photon-dance-delay-queue/internal/delay-queue"
)

type taskDelayQueueServiceServer struct {
	pb.UnimplementedTaskDelayQueueServiceServer
	dq *delayqueue.DelayQueue
}

func newTaskDelayQueueServiceServer(cfg *conf.DelayQueue) *taskDelayQueueServiceServer {
	return &taskDelayQueueServiceServer{
		dq: delayqueue.NewDelayQueue(cfg),
	}
}

func (srv *taskDelayQueueServiceServer) close() {
	srv.dq.Close()
}

func (srv *taskDelayQueueServiceServer) PushTask(ctx context.Context, req *pb.PushTaskRequest) (*pb.PushTaskResponse, error) {
	if req.GetTask().GetId() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty task id")
	}
	if req.GetTask().GetAttachedTopic() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty attached topic")
	}
	if req.GetTask().GetDelay() <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "invalid task delay")
	}
	if req.GetTask().GetTtr() <= 0 || req.GetTask().GetTtr() > 86400 {
		return nil, status.Errorf(codes.InvalidArgument, "empty attached topic")
	}

	task := &delayqueue.Task{}
	task.Id = req.GetTask().GetId()
	task.Topic = req.GetTask().GetAttachedTopic()
	task.Delay = time.Now().Unix() + int64(req.GetTask().GetDelay())
	task.TTR = int64(req.GetTask().GetTtr())
	task.Blob = req.GetTask().GetPayload()
	if err := srv.dq.Push(task); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.PushTaskResponse{}, nil
}

func (srv *taskDelayQueueServiceServer) FinishTask(ctx context.Context, req *pb.FinishTaskRequest) (*pb.FinishTaskResponse, error) {
	if req.GetTaskId() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty task id")
	}
	if err := srv.dq.Remove(req.GetTaskId()); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.FinishTaskResponse{}, nil
}

func (srv *taskDelayQueueServiceServer) CheckTask(ctx context.Context, req *pb.CheckTaskRequest) (*pb.CheckTaskResponse, error) {
	if req.GetTaskId() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty task id")
	}
	task, err := srv.dq.Get(req.GetTaskId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.CheckTaskResponse{
		Task: &pb.Task{
			Id:            task.Id,
			AttachedTopic: task.Topic,
			Ttr:           int32(task.TTR),
			Payload:       task.Blob,
		},
	}, nil
}

func (srv *taskDelayQueueServiceServer) SubscribeTopic(ctx context.Context, req *pb.SubscribeTopicRequest) (*pb.SubscribeTopicResponse, error) {
	if req.GetTopic() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty topic")
	}
	return &pb.SubscribeTopicResponse{}, nil
}

func (srv *taskDelayQueueServiceServer) UnsubscribeTopic(ctx context.Context, req *pb.UnsubscribeTopicRequest) (*pb.UnsubscribeTopicResponse, error) {
	if req.GetTopic() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty topic")
	}
	return &pb.UnsubscribeTopicResponse{}, nil
}
