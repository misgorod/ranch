import os
from ranch.acks import Acks
from ranch.order import Order
import sys


class FuncNotFoundError(Exception):
    pass


class Interpolation():
    def __init__(self, funcs):
        self.funcs = funcs

    def get_value(self, message_size: int, acks: Acks, order: Order) -> int:
        if not (float(acks.value), float(order.value)) in self.funcs:
            raise FuncNotFoundError
        func = self.funcs[(acks.value, order.value)]
        result = int(func(message_size))
        if result < 0:
            return 0
        return result