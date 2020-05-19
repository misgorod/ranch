import numpy as np
from .acks import Acks
from .order import Order


class Batch():
    def __init__(self):
        self.batch_funcs = np.load('batches.npy', allow_pickle=True)

    def get_batch(self, message_size: int, acks: Acks, order: Order) -> int:
        func = self.batch_funcs.item()[(acks.value, order.value)]
        batch_size = int(func(message_size))
        if batch_size < 0:
            return 0
        return batch_size