__author__ = "a.lemets"


class PercentMixin:
    @property
    def percent(self):
        if not hasattr(self, "_percent"):
            self._percent = 0
        return self._percent

    @percent.setter
    def percent(self, value):
        if value:
            self._percent = int(value)
