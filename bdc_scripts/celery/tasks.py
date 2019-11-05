# Python Native
import logging
import os
import time
from random import randint

# 3rdparty Libraries
from bdc_scripts.celery import celery_app

# BDC Scripts
from bdc_scripts.celery.cache import lock_handler
from bdc_scripts.sentinel.clients import sentinel_clients
from bdc_scripts.sentinel.download import download_sentinel_images
from bdc_scripts.sentinel.publish import publish
from bdc_scripts.core.utils import extractall, is_valid


lock = lock_handler.lock('sentinel_download_lock_4')


class SentinelTask(celery_app.Task):
    def get_user(self):
        """
        Tries to get an iddle user to download images.

        Since we are downloading images from Copernicus, you can only have
        two concurrent download per account. In this way, we should handle the
        access to the stack of SciHub accounts defined in `secrets_s2.json`
        in order to avoid download interrupt.

        Returns:
            AtomicUser An atomic user
        """
        user = None

        while lock.locked():
            logging.debug('Resource locked....')
            time.sleep(1)

        lock.acquire(blocking=True)
        while user is None:
            user = sentinel_clients.use()

            if user is None:
                logging.info('Waiting for available user to download...')
                time.sleep(1)

        lock.release()

        return user

    def download(self, scene):
        """
        Performs download sentinel images from copernicus

        Args:
            scene (dict) - Scene

        Returns:
            dict Scene with sentinel file path
        """

        # Acquire User to download
        with self.get_user() as user:
            logging.debug('Starting Download {}...'.format(user.username))

            cc = scene['scene_id'].split('_')
            year_month = cc[2][:4] + '-' + cc[2][4:6]

            # Output product dir
            product_dir = os.path.join(scene.get('destination'), year_month)
            link = scene['link']
            scene_id = scene['scene_id']

            zip_file_name = os.path.join(product_dir, '{}.zip'.format(scene_id))

            extracted_file_path = os.path.join(product_dir, '{}.SAFE'.format(scene_id))

            if not os.path.exists(extracted_file_path):
                valid = True

                if os.path.exists(zip_file_name):
                    valid = is_valid(zip_file_name)

                if not os.path.exists(zip_file_name) or not valid:
                    # Download from Copernicus
                    download_sentinel_images(link, zip_file_name, user)

                    # Check if file is valid
                    valid = is_valid(zip_file_name)

                if not valid:
                    os.remove(zip_file_name)
                    logging.error('Invalid zip file "{}"'.format(zip_file_name))
                    return None
                else:
                    extractall(zip_file_name)
            else:
                logging.info('Skipping download since the file {} already exists'.format(extracted_file_path))

            logging.debug('Done download.')

        scene.update(dict(
            file=extracted_file_path
        ))

        return scene

    def publish(self, scene):
        logging.debug('Starting Publish Sentinel...')

        publish(scene)

        logging.debug('Done Publish Sentinel.')

    def upload(self, scene):
        logging.debug('Starting Upload sentinel to AWS...')

        time.sleep(randint(4, 8))

        logging.debug('Done Upload sentinel to AWS.')


@celery_app.task(base=SentinelTask, queue='download')
def download_sentinel(scene):
    return download_sentinel.download(scene)


@celery_app.task(base=SentinelTask, queue='publish')
def publish_sentinel(scene):
    return publish_sentinel.publish(scene)


@celery_app.task(base=SentinelTask, queue='upload')
def upload_sentinel(scene):
    upload_sentinel.upload(scene)
