#!/bin/bash

echo "/**************************************************/"
echo "list all grpc services"
grpcurl -plaintext localhost:18081 list amazingchow.photon_dance_delay_queue.TaskDelayQueueService
echo "/**************************************************/"
echo "subscribe topic shopping_cart_service_line"
grpcurl -plaintext -d '{"topic": "shopping_cart_service_line"}' localhost:18081 amazingchow.photon_dance_delay_queue.TaskDelayQueueService/SubscribeTopic
echo "/**************************************************/"
echo "subscribe topic order_service_line"
grpcurl -plaintext -d '{"topic": "order_service_line"}' localhost:18081 amazingchow.photon_dance_delay_queue.TaskDelayQueueService/SubscribeTopic
echo "/**************************************************/"
echo "subscribe topic inventory_service_line"
grpcurl -plaintext -d '{"topic": "inventory_service_line"}' localhost:18081 amazingchow.photon_dance_delay_queue.TaskDelayQueueService/SubscribeTopic
echo "/**************************************************/"
echo "add one task"
cat task01.json | grpcurl -plaintext -d @ localhost:18081 amazingchow.photon_dance_delay_queue.TaskDelayQueueService/PushTask
echo "/**************************************************/"
echo "check the task"
grpcurl -plaintext -d '{"task_id": "fecb0b56-ff6c-4426-a257-d51781a9cd44"}' localhost:18081 amazingchow.photon_dance_delay_queue.TaskDelayQueueService/CheckTask
echo "/**************************************************/"
echo "add one task"
cat task02.json | grpcurl -plaintext -d @ localhost:18081 amazingchow.photon_dance_delay_queue.TaskDelayQueueService/PushTask
echo "/**************************************************/"
echo "check the task"
grpcurl -plaintext -d '{"task_id": "51e5b426-3f5f-4044-b469-713b56bceb3a"}' localhost:18081 amazingchow.photon_dance_delay_queue.TaskDelayQueueService/CheckTask
echo "/**************************************************/"
echo "add one task"
cat task03.json | grpcurl -plaintext -d @ localhost:18081 amazingchow.photon_dance_delay_queue.TaskDelayQueueService/PushTask
echo "/**************************************************/"
echo "check the task"
grpcurl -plaintext -d '{"task_id": "50365010-30c7-42a7-9550-9a65e57bc021"}' localhost:18081 amazingchow.photon_dance_delay_queue.TaskDelayQueueService/CheckTask
echo "/**************************************************/"