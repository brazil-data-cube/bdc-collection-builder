from datetime import datetime
import logging
from bdc_scripts.celery import celery_app
from .utils import warp as warp_processing, merge as merge_processing


@celery_app.task()
def warp(activity):
    args = activity.get('args', {})

    datacube = args.get('datacube')
    asset = args.get('asset')

    scene_date = datetime.strptime(asset['datetime'], '%Y-%m-%dT%H:%M:%S').date()

    warp_processing(asset['url'], asset['scene_id'], asset['tile'], asset['band'], datacube, scene_date, asset['feature_tile'])

    logging.warning('Execute Warp of {} - Asset {}'.format(datacube, asset.get('url')))

    return asset


@celery_app.task()
def warp_merge(datacube, tile_id, period, warps, cols, rows, **kwargs):
    logging.warning('Executing merge')

    merge_processing(datacube, tile_id, warps, int(cols), int(rows), period, **kwargs)


@celery_app.task()
def merge(warps, *args, **kwargs):
    logging.warning('Executing merge')


@celery_app.task()
def blend(merges, *args, **kwargs):
    logging.warning('Executing blend')


@celery_app.task()
def publish(blends, *args, **kwargs):
    logging.warning('Executing publish')
