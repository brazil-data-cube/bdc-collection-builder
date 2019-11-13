"""
Defines package for handling Radcor

This package contains definitions to work with satellite collections:
    - sentinel
    - landsat
"""

from bdc_scripts.celery.utils import TaskActivityFactory
from bdc_scripts.radcor.models import RadcorActivity


def create_activity(task, activity, *args, **kwargs):
    model = RadcorActivity(**activity)
    model.id = None
    model.task = task
    model.save()


# Register the factory handler
TaskActivityFactory.add('downloadS2', create_activity)
TaskActivityFactory.add('publishS2', create_activity)
TaskActivityFactory.add('uploadS2', create_activity)
TaskActivityFactory.add('downloadL8', create_activity)
TaskActivityFactory.add('publishL8', create_activity)
TaskActivityFactory.add('uploadL8', create_activity)