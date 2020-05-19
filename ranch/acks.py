from enum import Enum


class Acks(Enum):
    ALL = -1
    ONE = 1
    NO = 0