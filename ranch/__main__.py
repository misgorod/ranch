from .acks import Acks
from .batch import Batch
from .compression import get_compression
from .linger import Linger
from .order import Order

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient

from concurrent.futures import TimeoutError


def main():
    topic_name = 'test-snappy'
    kafka_admin = AdminClient({'bootstrap.servers': 'testing-kafka-dev01:9092'})
    batch = Batch()
    linger = Linger()
    try:
        compression_type = get_compression(kafka_admin, topic_name)
    except KafkaException as ke:
        error = ke.args[0]
        if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            print(f"topic with name {topic_name} doesn't exist")
        else:
            print(error.retriable())
            print(error.str())
        exit(1)
    except TimeoutError:
        print("timeout while connecting to broker")
        exit(1)
    batch_bytes = batch.get_batch(kafka_admin, 421412, Acks.NO, Order.ANY)
    linger_ms = linger.get_linger(kafka_admin, 421412, Acks.NO, Order.ANY)
    print(compression_type)
    print(batch_bytes)
    print(linger_ms)

if __name__ == '__main__':
    main()