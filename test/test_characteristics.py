from ranch.characteristics import Characteristics
from concurrent.futures import Future, TimeoutError
from confluent_kafka.admin import ConfigEntry, ConfigResource, ClusterMetadata
import pytest

class MockKafkaAdmin:
    def __init__(self, timeout):
        self.timeout = timeout

    def describe_configs(self, resources):
        if self.timeout:
            raise TimeoutError()
        future = Future()
        future.set_result({"compression.type": ConfigEntry("compression.type", "zstd"), "max.message.bytes": ConfigEntry("max.message.bytes", "100000")})
        return {ConfigResource(ConfigResource.Type.TOPIC, "test_topic"): future}

class MockKafkaConsumer:
    def list_topics(self):
        return MockTopics()
    
class MockTopics:
    def __init__(self):
        self.topics = {'test_topic': MockPartitions()}

class MockPartitions:
    def __init__(self):
        self.partitions = range(8)

def test_getTopicCompression_success():
    sut = Characteristics(MockKafkaAdmin(False), MockKafkaConsumer(), [], 'test_topic')
    actual = sut.get_topic_compression()
    assert actual == 'zstd'

def test_getTopicCompression_timeout():
    with pytest.raises(TimeoutError) as e:
        sut = Characteristics(MockKafkaAdmin(True), MockKafkaConsumer(), [], 'test_topic')
        sut.get_topic_compression()

def test_getTopicPartitionsCount_success():
    sut = Characteristics(MockKafkaAdmin(False), MockKafkaConsumer(), [], 'test_topic')
    actual = sut.get_topic_partitions_count()
    assert actual == 8
