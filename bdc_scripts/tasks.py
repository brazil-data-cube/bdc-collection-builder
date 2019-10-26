from random import randint
import time
from celery import Celery
import logging


app = Celery(__name__,
             backend='rpc://',
             broker='pyamqp://guest@localhost')


class TaskHandler(app.Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logging.error('{0!r} failed: {1!r}'.format(task_id, exc))


@app.task(base=TaskHandler, queue='download')
def download_sentinel():
    logging.info('Starting Download...')

    time.sleep(randint(3, 5))

    if randint(1, 10) == 1:
        raise TypeError('Error here')

    logging.info('Done download.')


@app.task(base=TaskHandler)
def publish_sentinel():
    logging.info('Publish Sentinel...')

    time.sleep(randint(10, 15))

    logging.info('Done Publish Sentinel.')


@app.task
def upload_sentinel():
    logging.info('Upload sentinel to AWS...')

    time.sleep(randint(4, 8))

    logging.info('Done Upload sentinel to AWS.')
