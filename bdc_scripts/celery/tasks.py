from random import randint
import logging
import time
from celery import Task
from bdc_scripts.celery import app
from bdc_scripts.celery.cache import lock_handler
from bdc_scripts.sentinel.clients import sentinel_clients


lock = lock_handler.lock('sentinel_download_lock_4')


class DownloadSentinelTask(Task):
    def get_user(self):
        user = None

        while lock.locked():
            logging.debug('Resource locked....')
            time.sleep(1)

        lock.acquire(blocking=True)
        while user is None:
            user = sentinel_clients.use()

            if user is None:
                logging.warning('Waiting for available user to download...')
                time.sleep(1)

        lock.release()

        return user

    def download(self):
        # Acquire User to download
        with self.get_user():
            logging.debug('Starting Download...')
            time.sleep(randint(5, 15))

            if randint(1, 10) == 1:
                raise TypeError('Error here')

            logging.debug('Done download.')


@app.task(base=DownloadSentinelTask, queue='download')
def download_sentinel():
    download_sentinel.download()


@app.task(queue='publish')
def publish_sentinel():
    logging.info('Publish Sentinel...')

    time.sleep(randint(10, 15))

    logging.info('Done Publish Sentinel.')


@app.task(queue='upload')
def upload_sentinel():
    logging.info('Upload sentinel to AWS...')

    time.sleep(randint(4, 8))

    logging.info('Done Upload sentinel to AWS.')
