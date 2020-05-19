from ranch.acks import Acks
from ranch.batch import Batch
from ranch.linger import Linger
from ranch.order import Order
from ranch.topic_config import TopicConfig
import os
import sys
from argparse import ArgumentParser
from concurrent.futures import TimeoutError
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient


def main():
    parser = ArgumentParser(description='computes configuration parameters for kafka producer based on librdkafka')

    parser.add_argument('brokers', help='address of kafka brokers in format host:port[,host:port]')
    parser.add_argument('topic', help='name of kafka topic')
    parser.add_argument('-m', '--message-size', help='average size of message sent to kafka in bytes', type=int, default=1024)
    parser.add_argument('-s', '--semantic', help='delivery semantic', choices=['exactly-once', 'at-least-once', 'at-most-once'], default='at-least-once')
    parser.add_argument('-o', '--strict-order', help='if enabled messages are delivered in the same order ther were produced', action='store_true', default=False)

    args = parser.parse_args()

    topic_name = args.topic
    kafka_admin = AdminClient({'bootstrap.servers': args.brokers})
    message_size = args.message_size

    acks = Acks.ONE
    idempotence = False
    if args.semantic == 'at-least-once':
        acks = Acks.ONE
    elif args.semantic == 'at-most-once':
        acks = Acks.NO
    else:
        acks = Acks.ALL
        idempotence = True

    inflight = Order.ANY
    if args.strict_order:
        inflight = Order.STRICT
    else:
        inflight = Order.ANY

    topic_config = TopicConfig(kafka_admin, topic_name)
    batch = Batch()
    linger = Linger()

    try:
        compression_type = topic_config.get_compression()
        max_message = topic_config.get_max_message()
    except KafkaException as ke:
        error = ke.args[0]
        if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            print(f"topic with name {topic_name} doesn't exist")
        else:
            print(error.str())
        sys.exit(1)
    except TimeoutError:
        print("timeout while connecting to broker")
        sys.exit(1)

    batch_bytes = batch.get_batch(message_size, acks, inflight)
    linger_ms = linger.get_linger(message_size, acks, inflight)

    if batch_bytes > max_message:
        batch_bytes = max_message

    print(f"bootstrap.servers={args.brokers}")
    print(f"acks={acks.value}")
    print(f"enable.idempotence={str(idempotence).lower()}")
    print(f"max.in.flight.requests.per.connection={inflight.value}")
    print(f"compression.type={compression_type}")
    print(f"message.max.bytes={max_message}")
    print(f"batch.size={batch_bytes}")
    print(f"linger.ms={linger_ms}")

if __name__ == '__main__':
    main()