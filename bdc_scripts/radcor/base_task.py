# Python Native
from datetime import datetime

# BDC Scripts
from bdc_scripts.celery import celery_app
from bdc_db.models import Collection, CollectionItem
from bdc_scripts.radcor.utils import get_or_create_model


class RadcorTask(celery_app.Task):
    def get_tile_id(self, scene_id, **kwargs) -> str:
        """Retrieves tile identifier from scene"""
        raise NotImplementedError()

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
            id=scene_id,
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
