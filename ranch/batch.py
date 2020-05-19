import numpy as np
import os
from ranch.acks import Acks
from ranch.order import Order
import sys


class Batch():
    def __init__(self):
        try:
            basepath = sys._MEIPASS
        except Exception:
            basepath = os.path.abspath(".")
        filename = 'funcs/batches.npy'
        filename = os.path.join(basepath, filename)
        self.batch_funcs = np.load(filename, allow_pickle=True)

    def get_batch(self, message_size: int, acks: Acks, order: Order) -> int:
        func = self.batch_funcs.item()[(acks.value, order.value)]
        batch_size = int(func(message_size))
        if batch_size < 0:
            return 0
        return batch_size