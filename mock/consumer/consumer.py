# -*- coding: utf-8 -*-
import grpc
import kafka
import pprint
import os
import sys
sys.path.append(os.path.abspath('./api'))

from api import task_delay_queue_service_pb2
from api import task_delay_queue_service_pb2_grpc


if __name__ == "__main__":
    grpc_channel = grpc.insecure_channel('localhost:18081')
    stub = task_delay_queue_service_pb2_grpc.TaskDelayQueueServiceStub(grpc_channel)

    consumer = kafka.KafkaConsumer(
        "inventory_service_line",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        group_id="mock_consumer_group_03",
        enable_auto_commit=True,
    )

    for msg in consumer:
        print("[%s:%d:%d]" % (msg.topic, msg.partition, msg.offset))
        value = msg.value
        t = task_delay_queue_service_pb2.Task()
        t.ParseFromString(value)
        pprint.pprint(t)
        # TODO: 请求可能会超时, 也许是因为主循环把外部请求给阻塞住了!!!
        req = task_delay_queue_service_pb2.FinishTaskRequest(task_id=t.id)
        stub.FinishTask(req)
