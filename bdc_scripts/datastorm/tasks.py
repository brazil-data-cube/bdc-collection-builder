from bdc_scripts.celery import celery_app
import logging


@celery_app.task()
def warp(activity):
    args = activity.get('args', {})

    datacube = args.get('datacube')
    asset = args.get('asset')

    logging.warning('Execute Warp of {} - Asset {}'.format(datacube, asset.get('url')))

    return asset


@celery_app.task()
def merge(warps, *args, **kwargs):
    logging.warning('Executing merge')


@celery_app.task()
def blend(merges, *args, **kwargs):
    logging.warning('Executing blend')


@celery_app.task()
def publish(blends, *args, **kwargs):
    logging.warning('Executing publish')
