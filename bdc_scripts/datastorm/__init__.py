"""
Defines package for handling Radcor

This package contains definitions to work with satellite collections:
    - sentinel
    - landsat
"""

from bdc_scripts.celery.utils import TaskActivityFactory
from bdc_scripts.datastorm.models import DataStormActivity, DataStormActivityHistory
from bdc_scripts.radcor.utils import get_or_create_model


def create_activity(task, activity, *args, **kwargs):
    """
    Creates a data storm activity once a celery task is received.
    Make sure to attach this function at TaskActivityFactory using
    activity type.

    Args:
        task (celery.backends.database.models.Task) - Celery Task model instance
        activity (dict) - data storm activity as dict
        *args - Arguments order
        **kwargs - Extra parameters
    """

    where = dict(
        sceneid=activity.get('sceneid'),
        activity_type=activity.get('activity_type'),
        collection_id=activity.get('collection_id'),
        band=activity.get('band')
    )

    activity.pop('history', None)
    activity.pop('id', None)
    activity.pop('last_execution', None)

    activity_model, _ = get_or_create_model(DataStormActivity, defaults=activity, **where)

    model = DataStormActivityHistory()

    model.task = task
    model.activity = activity_model
    model.save(commit=False)


# Register the factory handler
TaskActivityFactory.add('WARP', create_activity)
TaskActivityFactory.add('MERGE', create_activity)
