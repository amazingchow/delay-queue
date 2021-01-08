package main

import (
	"context"

	pb "github.com/amazingchow/photon-dance-delay-queue/api"
	conf "github.com/amazingchow/photon-dance-delay-queue/internal/config"
)

type taskDelayQueueServiceServer struct{}

func newTaskDelayQueueServiceServer(cfg *conf.DelayQueue) *taskDelayQueueServiceServer {
	return &taskDelayQueueServiceServer{}
}

func (srv *taskDelayQueueServiceServer) close() {

}

func (srv *taskDelayQueueServiceServer) Push(ctx context.Context, req *pb.PushTaskRequest) (*pb.PushTaskResponse, error) {
	return &pb.PushTaskResponse{}, nil
}

func (srv *taskDelayQueueServiceServer) Finish(ctx context.Context, req *pb.FinishTaskRequest) (*pb.FinishTaskResponse, error) {
	return &pb.FinishTaskResponse{}, nil
}

func (srv *taskDelayQueueServiceServer) Exist(ctx context.Context, req *pb.CheckTaskRequest) (*pb.CheckTaskResponse, error) {
	return &pb.CheckTaskResponse{}, nil
}
