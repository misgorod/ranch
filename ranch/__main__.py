from ranch.acks import Acks
from ranch.interpolation import Interpolation, FuncNotFoundError
from ranch.order import Order
from ranch.topic_config import TopicConfig
import os
import sys
import numpy as np
from argparse import ArgumentParser
from concurrent.futures import TimeoutError
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient


def arguments():
    parser = ArgumentParser(description='computes configuration parameters for kafka producer based on librdkafka')

    parser.add_argument('brokers', help='address of kafka brokers in format host:port[,host:port]')
    parser.add_argument('topic', help='name of kafka topic')
    parser.add_argument('-m', '--message-size', help='average size of message sent to kafka in bytes', type=int, default=1024)
    parser.add_argument('-s', '--semantic', help='delivery semantic', choices=['exactly-once', 'at-least-once', 'at-most-once'], default='at-least-once')
    parser.add_argument('-o', '--strict-order', help='if enabled messages are delivered in the same order ther were produced', action='store_true', default=False)

    args = parser.parse_args()

    return args.topic, args.brokers, args.message_size, args.semantic, args.strict_order


def resolve_acks(semantic):
    acks = Acks.ONE
    idempotence = False
    if semantic == 'at-least-once':
        acks = Acks.ONE
    elif semantic == 'at-most-once':
        acks = Acks.NO
    else:
        acks = Acks.ALL
        idempotence = True

    return acks, idempotence


def resolve_inflight(strict_order):
    inflight = Order.ANY
    if strict_order:
        inflight = Order.STRICT
    else:
        inflight = Order.ANY

    return inflight


def load_funcs():
    try:
        basepath = sys._MEIPASS
    except Exception:
        basepath = os.path.abspath(".")

    batch_file = 'funcs/batches.npy'
    linger_file = 'funcs/lingers.npy'
    batch_file = os.path.join(basepath, batch_file)
    linger_file = os.path.join(basepath, linger_file)
    batch_funcs = np.load(batch_file, allow_pickle=True)
    linger_funcs = np.load(linger_file, allow_pickle=True)

    return batch_funcs.item(), linger_funcs.item()


def print_config(brokers, acks, idempotence, inflight, compression_type, max_message, batch_bytes, linger_ms):
    print(f"bootstrap.servers={brokers}")
    print(f"acks={acks}")
    print(f"enable.idempotence={str(idempotence).lower()}")
    print(f"max.in.flight.requests.per.connection={inflight}")
    print(f"compression.type={compression_type}")
    print(f"message.max.bytes={max_message}")
    print(f"batch.size={batch_bytes}")
    print(f"linger.ms={linger_ms}")


def main():
    topic_name, brokers, message_size, semantic, strict_order = arguments()
    kafka_admin = AdminClient({'bootstrap.servers': brokers})

    acks, idempotence = resolve_acks(semantic)
    inflight = resolve_inflight(strict_order)

    topic_config = TopicConfig(kafka_admin, topic_name)

    batch_funcs, linger_funcs = load_funcs()
    batch_interpolation = Interpolation(batch_funcs)
    linger_interpolation = Interpolation(linger_funcs)

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
    except FuncNotFoundError:
        print("wrong value for acks or inflight")
        sys.exit(1)

    batch_bytes = batch_interpolation.get_value(message_size, acks, inflight)
    linger_ms = linger_interpolation.get_value(message_size, acks, inflight)

    if batch_bytes > max_message:
        batch_bytes = max_message

    print_config(brokers, acks.value, idempotence, inflight.value, compression_type, max_message, batch_bytes, linger_ms)

if __name__ == '__main__':
    main()