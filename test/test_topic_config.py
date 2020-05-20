from concurrent.futures import Future
from confluent_kafka.admin import ConfigResource, ConfigEntry
from ranch.topic_config import TopicConfig


class MockKafkaAdmin:
    def describe_configs(self, resources):
        future = Future()
        future.set_result({"compression.type": ConfigEntry("compression.type", "zstd"), "max.message.bytes": ConfigEntry("max.message.bytes", "100000")})
        return {ConfigResource(ConfigResource.Type.TOPIC, "test_topic"): future}

def test_getCompression_success():
    sut = TopicConfig(MockKafkaAdmin(), "test_topic")
    actual = sut.get_compression()
    assert actual == "zstd"

def test_getMaxMessage_success():
    sut = TopicConfig(MockKafkaAdmin(), "test_topic")
    actual = sut.get_max_message()
    assert actual == 100000