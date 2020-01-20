"""
Defines the utility functions to use among celery tasks
"""
from bdc_scripts.celery import celery_app


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


def list_running_tasks():
    """
    List all running tasks in celery cluster
    """

    inspector = celery_app.control.inspect()

    return inspector.active()


def list_pending_tasks():
    """
    List all pending tasks in celery cluster
    """

    inspector = celery_app.control.inspect()

    return inspector.reserved()
