"""
Defines the utility functions to use among celery tasks
"""


class TaskActivityFactory:
    _drivers = {}

    @classmethod
    def add(cls, name, driverFn):
        assert name not in cls._drivers

        cls._drivers[name] = driverFn

    @classmethod
    def get(cls, name):
        if name not in cls._drivers:
            return None

        return cls._drivers[name]
