from celery import current_app
from celery.app.control import Inspect


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


def list_active_tasks():
    inspector: Inspect = current_app.control.inspect()

    active_tasks = inspector.active()

    return [task for task in active_tasks]