"""
Defines the utility functions to use among celery tasks
"""
from bdc_collection_builder.celery import celery_app


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
