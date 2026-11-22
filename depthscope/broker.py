"""Ordered Kafka streams with explicit offset ownership."""

import json
import os
from uuid import uuid4

RAW = "depthscope.raw"
FRAMES = "depthscope.frames"
RESYNC = "depthscope.resync"
PROGRESS = "depthscope.progress"
CHECKPOINTS = "depthscope.checkpoints"


def settings():
    return {"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")}


def ensure_topics(*names, exclusive=False):
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import KafkaError, KafkaException

    admin = AdminClient(settings())
    topics = [NewTopic(name, 1, 1, config={"retention.ms": "86400000"} if ".replay." in name else {})
              for name in names]
    for name, future in admin.create_topics(topics, request_timeout=30).items():
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            if exclusive:
                raise ValueError(f"Kafka topic {name} already exists; use a fresh run ID") from exc


class Writer:
    def __init__(self):
        from confluent_kafka import Producer

        self.producer = Producer({**settings(), "enable.idempotence": True,
                                  "compression.type": "zstd", "message.max.bytes": 16_777_216})
        self.error = None

    def delivered(self, error, _message):
        if error is not None:
            self.error = error

    def send(self, topic, row):
        payload = json.dumps(row, allow_nan=False, separators=(",", ":")).encode()
        while True:
            if self.error is not None:
                raise RuntimeError(str(self.error))
            try:
                self.producer.produce(topic, payload, partition=0, on_delivery=self.delivered)
                self.producer.poll(0)
                return
            except BufferError:
                self.producer.poll(0.2)

    def flush(self):
        if self.producer.flush(30) or self.error:
            raise RuntimeError(f"Kafka publish failed: {self.error or 'timeout'}")


class Reader:
    def __init__(self, topic, offset=None, tail=False):
        from confluent_kafka import Consumer, TopicPartition

        self.consumer = Consumer({**settings(), "group.id": f"depthscope-{uuid4()}",
                                  "enable.auto.commit": False, "fetch.message.max.bytes": 16_777_216})
        partition = TopicPartition(topic, 0)
        low, high = self.consumer.get_watermark_offsets(partition, timeout=20)
        if offset is not None and not low <= offset <= high:
            self.consumer.close()
            raise RuntimeError(f"{topic} checkpoint {offset} outside retained offsets [{low}, {high}]")
        partition.offset = offset if offset is not None else (high if tail else low)
        self.consumer.assign([partition])

    def poll(self, timeout=1.0):
        message = self.consumer.poll(timeout)
        if message is None:
            return None
        if message.error():
            raise RuntimeError(str(message.error()))
        return message.offset(), json.loads(message.value())

    def close(self):
        self.consumer.close()
