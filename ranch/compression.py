from confluent_kafka.admin import ConfigResource
from confluent_kafka import KafkaError
from retrying import retry


class TopicConfig():
    def __init__(self, kafka_admin, topic_name):
        self.kafka_admin = kafka_admin

    def get_compression(self, topic_name: str) -> str:
        config_resource = ConfigResource(ConfigResource.Type.TOPIC, topic_name)
        config_futures = self.kafka_admin.describe_configs([config_resource])
        future = config_futures[config_resource]
        config = future.result(timeout=5)
        compression_config = config['compression.type']
        compression_type = compression_config.value

        if compression_type in ['snappy', 'gzip', 'lz4', 'zstd']:
            return compression_type
        else:
            return 'snappy'

    def get_max_request(self)
