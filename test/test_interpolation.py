import pytest
from ranch.interpolation import Interpolation, FuncNotFoundError
from ranch.enums import Acks, Order

def mock_func(t):
    return 0
funcs = {(1, 1): mock_func}

def test_getValue_success():
    sut = Interpolation(funcs)
    actual = sut.get_value(1, Acks.ONE, Order.STRICT)
    assert actual == 0

def test_getValue_notFound():
    sut = Interpolation(funcs)
    with pytest.raises(FuncNotFoundError):
        actual = sut.get_value(1, Acks.NO, Order.ANY)