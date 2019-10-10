from celery import Celery
from rc_maestro.sentinel import publish as publishS2


celery = Celery(__name__,
                backend='rpc://',
                broker='pyamqp://guest@localhost')


@celery.task
def download_sentinel(activity):
    print('Download sentinel... COGS')

    # upload_sentinel.delay(activity)


@celery.task
def publish_sentinel(activity):
    print('Publishing sentinel... COGS')

    return publishS2(activity)


@celery.task
def upload_sentinel(activity):
    print('Upload sentinel to AWS')


@celery.task
def publish_landsat(activity):
    print('Publishing landsat8... COGS')

    # upload_sentinel.delay(activity)


@celery.task
def upload_landsat(activity):
    print('Upload landsat8 to AWS')


@celery.task
def download_landsat(activity):
    print('Download sentinel... COGS')

    # upload_sentinel.delay(activity)