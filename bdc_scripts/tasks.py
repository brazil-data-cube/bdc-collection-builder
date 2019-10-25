from random import randint
import time
from celery import Celery
import logging


celery = Celery(__name__,
                backend='rpc://',
                broker='pyamqp://guest@localhost')


class TaskHandler(celery.Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print('{0!r} failed: {1!r}'.format(task_id, exc))


@celery.task(base=TaskHandler, exchange='download_sentinel')
def download_sentinel():
    current_id = celery.AsyncResult.task_id

    logging.info('Starting Download {}...'.format(current_id))

    time.sleep(randint(10, 15))

    logging.info('Done download {}...'.format(current_id))


@celery.task(base=TaskHandler)
def publish_sentinel():
    current_id = celery.AsyncResult.task_id

    logging.info('Publish Sentinel {}...'.format(current_id))

    time.sleep(randint(10, 15))

    logging.info('Done Publish Sentinel {}...'.format(current_id))


@celery.task
def upload_sentinel():
    current_id = celery.AsyncResult.task_id

    logging.info('Upload sentinel to AWS {}...'.format(current_id))

    time.sleep(randint(4, 8))

    logging.info('Done Upload sentinel to AWS {}...'.format(current_id))
