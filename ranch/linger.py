import numpy as np
import os
from ranch.acks import Acks
from ranch.order import Order
import sys


class Linger():
    def __init__(self):
        try:
            basepath = sys._MEIPASS
        except Exception:
            basepath = os.path.abspath(".")
        filename = 'funcs/lingers.npy'
        filename = os.path.join(basepath, filename)
        self.linger_funcs = np.load(filename, allow_pickle=True)

    def get_linger(self, message_size: int, acks: Acks, order: Order) -> int:
        func = self.linger_funcs.item()[(acks.value, order.value)]
        linger = int(func(message_size))
        if linger < 0:
            return 0
        return linger