import logging
import random
import string

from confluent_kafka import Producer, Consumer

from settings import KAFKA_BROKER


def create_producer():
    try:
        letters = string.ascii_lowercase
        client_id = ''.join(random.choice(letters) for i in range(10))
        producer = Producer({"bootstrap.servers": KAFKA_BROKER,
                             "client.id": client_id,
                             "broker.address.family": "v4",
                             "enable.idempotence": True,  # EOS processing
                             "compression.type": "lz4",
                             "batch.size": 64000,
                             "linger.ms": 10,
                             "acks": "all",  # Wait for the leader and all ISR to send response back
                             "retries": 5,
                             "delivery.timeout.ms": 1000})  # Total time to make retries
    except Exception as _:
        logging.exception("Couldn't create the producer")
        producer = None
    return producer


def create_consumer(topic, group_id):
    try:
        letters = string.ascii_lowercase
        client_id = ''.join(random.choice(letters) for i in range(10))
        group_id = ''.join(random.choice(letters) for i in range(10))
        consumer = Consumer({"bootstrap.servers": KAFKA_BROKER,
                             "broker.address.family": "v4",
                             "group.id": group_id,
                             "client.id": client_id,
                             "isolation.level": "read_committed",
                             "default.topic.config": {"auto.offset.reset": "latest",  # Only consume new messages
                                                      "enable.auto.commit": False}
                             })
        consumer.subscribe([topic])
    except Exception as _:
        logging.exception("Couldn't create the consumer")
        consumer = None

    return consumer
