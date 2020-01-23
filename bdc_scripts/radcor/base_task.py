# Python Native
from datetime import datetime
from os import environ

# 3rdparty
from celery import current_task
from celery.backends.database import Task

# BDC Scripts
from bdc_scripts.celery import celery_app
from bdc_db.models import Collection, CollectionItem
from bdc_scripts.radcor.models import RadcorActivity, RadcorActivityHistory
from bdc_scripts.radcor.utils import get_or_create_model


class RadcorTask(celery_app.Task):
    def get_tile_id(self, scene_id, **kwargs) -> str:
        """Retrieves tile identifier from scene"""
        raise NotImplementedError()

    def create_execution(self, activity):
        """
        Creates a radcor activity once a celery task is running.

        Args:
            activity (dict) - Radcor activity as dict
        """

        where = dict(
            sceneid=activity.get('sceneid'),
            activity_type=activity.get('activity_type'),
            collection_id=activity.get('collection_id')
        )

        activity.pop('history', None)
        activity.pop('id', None)
        activity.pop('last_execution', None)

        activity_model, _ = get_or_create_model(RadcorActivity, defaults=activity, **where)

        # Ensure that args values is always updated
        activity_model.args = activity['args']

        model = RadcorActivityHistory()

        task, _ = get_or_create_model(Task, defaults={}, task_id=current_task.request.id)

        model.task = task
        model.activity = activity_model
        model.start = datetime.utcnow()
        model.env = dict(environ)
        model.save()

        return model

    def get_tile_date(self, scene_id, **kwargs) -> datetime:
        """Retrieves the respective date from scene"""
        raise NotImplementedError()

    def get_collection(self, activity) -> Collection:
        """Retrieve the collection associated with BDC Scripts Activity"""
        return Collection.query().filter(Collection.id == activity.collection_id).one()

    def get_collection_item(self, activity) -> CollectionItem:
        """
        Retrieves a collection item using activity.
        It tries to add into db session scope a new one if no collection item is
        found.
        """
        scene_id = activity.sceneid

        collection = self.get_collection(activity)

        composite_date = self.get_tile_date(scene_id)

        restriction = dict(
            id='{}-{}'.format(collection.id, scene_id),
            tile_id=self.get_tile_id(scene_id),
            collection_id=collection.id,
            grs_schema_id=collection.grs_schema_id,
            item_date=composite_date.date()
        )

        collection_params = dict(
            composite_start=composite_date,
            composite_end=composite_date,
            cloud_cover=activity.args.get('cloud'),
            scene_type='SCENE',
            **restriction
        )

        collection_item, _ = get_or_create_model(CollectionItem, defaults=collection_params, **restriction)

        return collection_item
