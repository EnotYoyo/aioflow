from aioflow import PercentMixin

__author__ = "a.lemets"


def test_percent_mixin():
    class TestClass(PercentMixin):
        ...

    obj = TestClass()
    assert obj.percent == 0
    obj.percent = 35
    assert obj.percent == 35
