class Processor:
    def __init__(self, characteristics, batch_interpolation, linger_interpolation, message_size, acks, threads):
        self.characteristics = characteristics
        self.batch_interpolation = batch_interpolation
        self.linger_interpolation = linger_interpolation
        self.message_size = message_size
        self.acks = acks
        self.threads = threads

    def get_compression(self):
        compression = self.characteristics.get_topic_compression()
        if compression == 'uncompressed':
            return 'snappy'
        return compression

    def get_batch_size(self):
        return self.batch_interpolation.get_value(self.message_size, self.acks, self.threads)

    def get_linger_ms(self):
        return self.linger_interpolation.get_value(self.message_size, self.acks, self.threads)

    def get_max_buffer(self):
        ram = self.characteristics.get_ram()
        partitions_count = self.characteristics.get_topic_partitions_count()
        batch_size = self.get_batch_size()
        threshold = ram / 5
        if batch_size * partitions_count > threshold:
            return threshold
        return batch_size * partitions_count
