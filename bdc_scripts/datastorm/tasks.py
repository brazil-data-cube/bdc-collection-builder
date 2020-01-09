from celery import chain
from datetime import datetime
import logging
from bdc_scripts.celery import celery_app
from .utils import warp as warp_processing, merge as merge_processing, blend as blend_processing


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
def warp_merge(warped_datacube, tile_id, period, warps, cols, rows, **kwargs):
    logging.warning('Executing merge')

    return merge_processing(warped_datacube, tile_id, warps, int(cols), int(rows), period, **kwargs)


@celery_app.task()
def merge(warps, *args, **kwargs):
    logging.warning('Executing merge')


@celery_app.task()
def blend(merges):
    activities = {}

    for _merge in merges:
        if _merge['band'] in activities:
            continue

        activity = dict(scenes=dict())
        activity['datacube'] = merges[0]['datacube']
        activity['band'] = _merge['band']
        activity['scenes'].setdefault(_merge['date'], dict(**_merge))
        activity['period'] = _merge['period']
        activity['tile_id'] = _merge['tile_id']

        activity['scenes'][_merge['date']]['ARDfiles'] = {
            "quality": _merge['file'].replace(_merge['band'], 'quality'),
            _merge['band']: _merge['file']
        }

        activities[_merge['band']] = activity

    logging.warning('Scheduling blend....')

    for activity in activities.values():
        task = chain(_blend.s(activity))
        task.apply_async()


@celery_app.task()
def _blend(activity):
    blend_processing(activity)

    logging.warning('Executing blend')


@celery_app.task()
def publish(blends, *args, **kwargs):
    logging.warning('Executing publish')
