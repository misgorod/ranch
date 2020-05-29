import random
import psutil
import tcp_latency
import confluent_kafka.admin as adm

class Characteristics:
    def __init__(self, kafka_admin, consumer, brokers, topic):
        self.kafka_admin = kafka_admin
        self.consumer = consumer
        self.topic = topic
        self.brokers = brokers
        self.topic_config = self._get_topic_config()

    def _get_topic_config(self):
        config_resource = adm.ConfigResource(adm.ConfigResource.Type.TOPIC, self.topic)
        config_futures = self.kafka_admin.describe_configs([config_resource])
        future = config_futures[config_resource]
        return future.result(timeout=30)

    def get_ram(self):
        vmem = psutil.virtual_memory()
        return vmem.total

    def get_topic_compression(self):
        return self.topic_config['compression.type'].value

    def get_average_latency(self):
        latencies = [tcp_latency.measure_latency(host=host, port=9092, runs=1)[0] for host in self.brokers]
        return sum(latencies)/len(latencies)

    def get_topic_partitions_count(self):
        cluster_metadata = self.consumer.list_topics()
        topic_metadata = cluster_metadata.topics[self.topic]
        return len(topic_metadata.partitions)
