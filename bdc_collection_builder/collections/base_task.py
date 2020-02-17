#
# This file is part of BDC Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# BDC Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Define base interface for Celery Tasks."""


# Python Native
from datetime import datetime
from os import environ
# 3rdparty
from celery import current_task
from celery.backends.database import Task
# Builder
from bdc_collection_builder.celery import celery_app
from bdc_db.models import Collection, CollectionItem
from .models import RadcorActivity, RadcorActivityHistory
from .utils import get_or_create_model


class RadcorTask(celery_app.Task):
    """Define base task for celery execution."""

    def get_tile_id(self, scene_id, **kwargs) -> str:
        """Retrieve tile identifier from scene."""
        raise NotImplementedError()

    def create_execution(self, activity):
        """Create a radcor activity once a celery task is running.

        Args:
            activity (dict) - Radcor activity as dict
        """
        model = RadcorActivityHistory.query().filter(
            RadcorActivityHistory.task.has(task_id=current_task.request.id)
        ).first()

        if model is None:
            where = dict(
                sceneid=activity.get('sceneid'),
                activity_type=activity.get('activity_type'),
                collection_id=activity.get('collection_id')
            )

            activity.pop('history', None)
            activity.pop('id', None)
            activity.pop('last_execution', None)

            activity_model, _ = get_or_create_model(RadcorActivity, defaults=activity, **where)

            model = RadcorActivityHistory()

            task, _ = get_or_create_model(Task, defaults={}, task_id=current_task.request.id)

            model.task = task
            model.activity = activity_model
            model.start = datetime.utcnow()
            model.env = dict(environ)

        # Ensure that args values is always updated
        model.activity.args = activity['args']

        model.save()

        return model

    def get_tile_date(self, scene_id, **kwargs) -> datetime:
        """Retrieve the respective date from scene."""
        raise NotImplementedError()

    def get_collection(self, activity) -> Collection:
        """Retrieve the collection associated with Builder Activity."""
        return Collection.query().filter(Collection.id == activity.collection_id).one()

    def get_collection_item(self, activity) -> CollectionItem:
        """Retrieve a collection item using activity.

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
