# -*- coding: utf-8 -*-
import kafka
import pprint
import os
import sys
sys.path.append(os.path.abspath('./api'))

from api.task_delay_queue_service_pb2 import Task


if __name__ == "__main__":
    consumer = kafka.KafkaConsumer(
        "inventory_service_line",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        group_id="mock_consumer_group_01",
        enable_auto_commit=True,
    )

    for msg in consumer:
        print("[%s:%d:%d]" % (msg.topic, msg.partition, msg.offset))
        value = msg.value
        t = Task()
        t.ParseFromString(value)
        pprint.pprint(t)
        # TODO: send finish-task-grpc-request to delay-queue
