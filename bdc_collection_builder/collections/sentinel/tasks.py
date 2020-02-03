"""
Describes the Celery Tasks definition of Sentinel products
"""

# Python Native
import logging
import os
import time
from datetime import datetime
from zipfile import ZipFile

# 3rdparty
from psycopg2 import InternalError
from requests.exceptions import ConnectionError, HTTPError
from sqlalchemy.exc import InvalidRequestError

# BDC DB
from bdc_db.models import db

# Builder
from bdc_collection_builder.celery import celery_app
from bdc_collection_builder.celery.cache import lock_handler
from bdc_collection_builder.core.utils import extractall, is_valid, upload_file
from bdc_collection_builder.config import Config
from bdc_collection_builder.collections.base_task import RadcorTask
from bdc_collection_builder.collections.sentinel.clients import sentinel_clients
from bdc_collection_builder.collections.sentinel.download import download_sentinel_images, download_sentinel_from_creodias
from bdc_collection_builder.collections.sentinel.publish import publish
from bdc_collection_builder.collections.sentinel.correction import correction_sen2cor255, correction_sen2cor280


lock = lock_handler.lock('sentinel_download_lock_4')


class SentinelTask(RadcorTask):
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
            logging.info('Resource locked....')
            time.sleep(1)

        lock.acquire(blocking=True)
        while user is None:
            user = sentinel_clients.use()

            if user is None:
                logging.info('Waiting for available user to download...')
                time.sleep(5)

        lock.release()

        return user

    def get_tile_id(self, scene_id, **kwargs):
        fragments = scene_id.split('_')
        return fragments[-2][1:]

    def get_tile_date(self, scene_id, **kwargs):
        fragments = scene_id.split('_')

        # Retrieve composite date of Collection Item
        return datetime.strptime(fragments[2][:8], '%Y%m%d')

    def download(self, scene):
        """
        Performs download sentinel images from copernicus

        Args:
            scene (dict) - Scene containing activity

        Returns:
            dict Scene with sentinel file path
        """

        scene['collection_id'] = 'S2TOA'

        # Create/update activity
        activity_history = self.create_execution(scene)

        with db.session.no_autoflush:
            activity_args = scene.get('args', dict())

            collection_item = self.get_collection_item(activity_history.activity)

            fragments = scene['sceneid'].split('_')
            year_month = fragments[2][:4] + '-' + fragments[2][4:6]

            # Output product dir
            product_dir = os.path.join(activity_args.get('file'), year_month)
            link = activity_args['link']
            scene_id = scene['sceneid']

            zip_file_name = os.path.join(product_dir, '{}.zip'.format(scene_id))

            collection_item.compressed_file = zip_file_name.replace(Config.DATA_DIR, '')
            cloud = activity_args.get('cloud')

            if cloud:
                collection_item.cloud_cover = cloud

            try:
                valid = True

                if os.path.exists(zip_file_name):
                    logging.debug('zip file exists')
                    valid = is_valid(zip_file_name)

                if not os.path.exists(zip_file_name) or not valid:
                    try:
                        # Acquire User to download
                        with self.get_user() as user:
                            logging.info('Starting Download {} - {}...'.format(scene_id, user.username))
                            # Download from Copernicus
                            download_sentinel_images(link, zip_file_name, user)
                    except (ConnectionError, HTTPError) as e:
                        try:
                            logging.warning('Trying to download "{}" from external provider...'.format(scene_id))

                            download_sentinel_from_creodias(scene_id, zip_file_name)
                        except:
                            # Ignore errors from external provider
                            raise e

                    # Check if file is valid
                    valid = is_valid(zip_file_name)

                if not valid:
                    raise IOError('Invalid zip file "{}"'.format(zip_file_name))
                else:
                    extractall(zip_file_name)

                # Get extracted zip folder name
                with ZipFile(zip_file_name) as zipObj:
                    listOfiles = zipObj.namelist()
                    extracted_file_path = os.path.join(product_dir, '{}'.format(listOfiles[0]))[:-1]

                logging.debug('Done download.')
                activity_args['file'] = extracted_file_path

            except HTTPError as e:
                if os.path.exists(zip_file_name):
                    os.remove(zip_file_name)
                # Retry when sentinel is offline
                logging.warning('Sentinel "{}" is offline. Retrying in {}'.format(scene_id, Config.TASK_RETRY_DELAY))

                raise e

            except ConnectionError as e:
                logging.error('Connection error - {}'.format(e))
                if os.path.exists(zip_file_name):
                    os.remove(zip_file_name)
                # Retry when sentinel is offline
                raise e

            except BaseException as e:
                logging.error('An error occurred during task execution {}'.format(e))

                raise e

        # Persist a collection item on database
        collection_item.save()

        activity_args.pop('link')
        scene['args'] = activity_args

        # Create new activity 'correctionS2' to continue task chain
        scene['activity_type'] = 'correctionS2'

        return scene

    def correction(self, scene):
        logging.debug('Starting Correction Sentinel...')
        version = 'sen2cor280'

        # Set Collection to the Sentinel Surface Reflectance
        scene['collection_id'] = 'S2SR_SEN28'
        scene['activity_type'] = 'correctionS2'

        # Create/update activity
        self.create_execution(scene)

        try:
            params = dict(
                app=scene['activity_type'],
                sceneid=scene['sceneid'],
                file=scene['args']['file']
            )

            if version == 'sen2cor280':
                correction_result = correction_sen2cor280(params)
            else:
                correction_result = correction_sen2cor255(params)
            if correction_result is not None:
                scene['args']['file'] = correction_result

        except BaseException as e:
            logging.error('An error occurred during task execution - {}'.format(scene.get('sceneid')))
            raise e

        scene['activity_type'] = 'publishS2'

        return scene

    def publish(self, scene):
        scene['activity_type'] = 'publishS2'

        # Create/update activity
        activity_history = self.create_execution(scene)

        logging.info('Starting publish Sentinel {} - Activity {}'.format(
            scene.get('collection_id'),
            activity_history.activity.id
        ))

        try:
            assets = publish(self.get_collection_item(activity_history.activity), activity_history.activity)
        except BaseException as e:
            logging.error('An error occurred during task execution', e)
            raise e

        # Create new activity 'uploadS2' to continue task chain
        scene['activity_type'] = 'uploadS2'
        scene['args']['assets'] = assets

        logging.debug('Done Publish Sentinel.')

        return scene

    def upload(self, scene):
        # Create/update activity
        self.create_execution(scene)

        assets = scene['args']['assets']

        for entry in assets.values():
            file_without_prefix = entry['asset'].replace('{}/'.format(Config.AWS_BUCKET_NAME), '')
            logging.warning('Uploading {} to BUCKET {} - {}'.format(entry['file'], Config.AWS_BUCKET_NAME, file_without_prefix))
            upload_file(entry['file'], Config.AWS_BUCKET_NAME, file_without_prefix)


# TODO: Sometimes, copernicus reject the connection even using only 2 concurrent connection
# We should set "autoretry_for" and retry_kwargs={'max_retries': 3} to retry
# task execution since it seems to be bug related to the api
@celery_app.task(base=SentinelTask, queue='download', max_retries=72, autoretry_for=(HTTPError,), default_retry_delay=Config.TASK_RETRY_DELAY)
def download_sentinel(scene):
    """
    Represents a celery task definition for handling Sentinel Download files

    This celery tasks listen only for queues 'download'.

    Args:
        scene (dict): Radcor Activity

    Returns:
        Returns processed activity
    """

    return download_sentinel.download(scene)


@celery_app.task(base=SentinelTask, queue='atm-correction', max_retries=3, default_retry_delay=Config.TASK_RETRY_DELAY)
def atm_correction(scene):
    """
    Represents a celery task definition for handling Sentinel
    Atmospheric correction - sen2cor.

    This celery tasks listen only for queues 'atm-correction'.

    It only calls sen2cor for L1C products. It skips for
    sentinel L2A.

    Args:
        scene (dict): Radcor Activity with "correctionS2" app context

    Returns:
        Returns processed activity
    """

    return atm_correction.correction(scene)


@celery_app.task(base=SentinelTask, queue='publish', max_retries=3, autoretry_for=(InternalError, InvalidRequestError,), default_retry_delay=Config.TASK_RETRY_DELAY)
def publish_sentinel(scene):
    """
    Represents a celery task definition for handling Sentinel
    Publish TIFF files generation

    This celery tasks listen only for queues 'publish'.

    Args:
        scene (dict): Radcor Activity with "publishS2" app context

    Returns:
        Returns processed activity
    """

    return publish_sentinel.publish(scene)


@celery_app.task(base=SentinelTask, queue='upload', max_retries=3, default_retry_delay=Config.TASK_RETRY_DELAY)
def upload_sentinel(scene):
    """
    Represents a celery task definition for handling Sentinel
    Publish TIFF files generation

    This celery tasks listen only for queues 'publish'.

    Args:
        scene (dict): Radcor Activity with "publishS2" app context

    Returns:
        Returns processed activity
    """

    upload_sentinel.upload(scene)
