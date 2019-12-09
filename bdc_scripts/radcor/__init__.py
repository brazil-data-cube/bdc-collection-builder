"""
Defines package for handling Radcor

This package contains definitions to work with satellite collections:
    - sentinel
    - landsat
"""

from bdc_scripts.celery.utils import TaskActivityFactory
from bdc_scripts.radcor.models import RadcorActivity, RadcorActivityHistory
from bdc_scripts.radcor.utils import get_or_create_model


def create_activity(task, activity, *args, **kwargs):
    """
    Creates a radcor activity once a celery task is received.
    Make sure to attach this function at TaskActivityFactory using
    activity type.

    Args:
        task (celery.backends.database.models.Task) - Celery Task model instance
        activity (dict) - Radcor activity as dict
        *args - Arguments order
        **kwargs - Extra parameters
    """

    where = dict(
        sceneid=activity.get('sceneid'),
        activity_type=activity.get('activity_type'),
        collection_id=activity.get('collection_id')
    )

    if activity.get('history'):
        del activity['history']

    if activity.get('id'):
        del activity['id']

    activity_model, _ = get_or_create_model(RadcorActivity, defaults=activity, **where)

    model = RadcorActivityHistory()

    model.task = task
    model.activity = activity_model
    model.save(commit=False)


# Register the factory handler
TaskActivityFactory.add('downloadS2', create_activity)
TaskActivityFactory.add('correctionS2', create_activity)
TaskActivityFactory.add('publishS2', create_activity)
TaskActivityFactory.add('uploadS2', create_activity)

TaskActivityFactory.add('downloadLC8', create_activity)
TaskActivityFactory.add('correctionLC8', create_activity)
TaskActivityFactory.add('publishLC8', create_activity)
TaskActivityFactory.add('uploadLC8', create_activity)
