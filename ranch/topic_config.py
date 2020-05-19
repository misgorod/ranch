from confluent_kafka.admin import ConfigResource
from confluent_kafka import KafkaError
from retrying import retry
import json


class TopicConfig():
    def __init__(self, kafka_admin, topic_name: str):
        self.kafka_admin = kafka_admin
        self.topic_name = topic_name

    def get_compression(self) -> str:
        config_resource = ConfigResource(ConfigResource.Type.TOPIC, self.topic_name)
        config_futures = self.kafka_admin.describe_configs([config_resource])
        future = config_futures[config_resource]
        config = future.result(timeout=5)
        compression_config = config['compression.type']
        compression_type = compression_config.value

        if compression_type in ['snappy', 'gzip', 'lz4', 'zstd']:
            return compression_type
        else:
            return 'snappy'

    def get_max_message(self) -> int:
        config_resource = ConfigResource(ConfigResource.Type.TOPIC, self.topic_name)
        config_futures = self.kafka_admin.describe_configs([config_resource])
        future = config_futures[config_resource]
        config = future.result(timeout=5)
        max_message_config = config['max.message.bytes']
        return int(max_message_config.value)
