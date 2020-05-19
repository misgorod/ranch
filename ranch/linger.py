import numpy as np
from .acks import Acks
from .order import Order


class Linger():
    def __init__(self):
        self.linger_funcs = np.load('lingers.npy', allow_pickle=True)

    def get_linger(self, message_size: int, acks: Acks, order: Order) -> int:
        func = self.linger_funcs.item()[(acks.value, order.value)]
        linger = int(func(message_size))
        if linger < 0:
            return 0
        return linger