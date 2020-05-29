from confluent_kafka.admin import ConfigResource, ConfigEntry
from ranch.processor import Processor
from ranch.enums import Acks, Order

class MockCharacteristics:
    def get_topic_compression(self):
        return 'zstd'

class MockInterpolation:
    def get_value(self, message_size, acks, threads):
        return 123

def test_getCompression_success():
    sut = Processor(MockCharacteristics(), MockInterpolation(), MockInterpolation(), 123, Acks.NO, Order.ANY)
    actual = sut.get_compression()
    assert actual == "zstd"

def test_getBatchSize_success():
    sut = Processor(MockCharacteristics(), MockInterpolation(), MockInterpolation(), 123, Acks.NO, Order.ANY)
    actual = sut.get_batch_size()
    assert actual == 123

def test_getLingerMs_success():
    sut = Processor(MockCharacteristics(), MockInterpolation(), MockInterpolation(), 123, Acks.NO, Order.ANY)
    actual = sut.get_linger_ms()
    assert actual == 123