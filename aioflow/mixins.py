__author__ = "a.lemets"


class PercentMixin:
    @property
    def percent(self):
        return getattr(self, "_percent", 0)

    @percent.setter
    def percent(self, value: int):
        if value:
            setattr(self, "_percent", int(value))
