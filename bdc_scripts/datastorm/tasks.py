# Python Native
from datetime import datetime
import logging
# 3rdparty
from celery import chain, group
# BDC Scripts
from bdc_scripts.celery import celery_app
from .utils import merge as merge_processing, blend as blend_processing


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

    blends = []

    for activity in activities.values():
        blends.append(_blend.s(activity))

    task = chain(group(blends), publish.s())
    task.apply()


@celery_app.task()
def _blend(activity):
    logging.warning('Executing blend')

    return blend_processing(activity)


@celery_app.task()
def publish(blends):
    logging.warning('Executing publish')
