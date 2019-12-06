"""
Describes the Celery Tasks definition of Sentinel products
"""

# Python Native
import glob
import logging
import os
import shutil
import time
from datetime import datetime
from json import loads as json_parser

# 3rdparty
from requests.exceptions import ConnectionError
from requests import get as resource_get

# BDC DB
from bdc_db.models import CollectionItem

# BDC Scripts
from bdc_scripts.config import Config
from bdc_scripts.celery import celery_app
from bdc_scripts.celery.cache import lock_handler
from bdc_scripts.core.utils import extractall, is_valid
from bdc_scripts.radcor.sentinel.clients import sentinel_clients
from bdc_scripts.radcor.sentinel.download import download_sentinel_images
from bdc_scripts.radcor.sentinel.publish import publish
from bdc_scripts.radcor.sentinel.correction import correction_sen2cor255, correction_sen2cor280, search_recent_sen2cor280
from bdc_scripts.radcor.utils import get_task_activity


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
            # Persist the activity to done
            activity_history = get_task_activity()
            activity_history.activity.status = 'DOING'
            activity_history.start = datetime.utcnow()
            activity_history.save()

            logging.debug('Starting Download {}...'.format(user.username))

            fragments = scene['sceneid'].split('_')
            year_month = fragments[2][:4] + '-' + fragments[2][4:6]
            tile_id = fragments[-2]

            # S2B_MSIL1C_20170920T133209_N0205_R081_T22LGR_20170920T133212
            composite_date = datetime.strptime(fragments[2][:8], '%Y-%m-%dT')

            collection_item = CollectionItem(
                tile_id=tile_id,
                composite_start=composite_date,
                composite_end=composite_date,
                cloud_cover=scene.get('cloud'),
                scene_type='SCENE'
            )

            # Output product dir
            product_dir = os.path.join(scene.get('file'), year_month)
            link = scene['link']
            scene_id = scene['sceneid']

            zip_file_name = os.path.join(product_dir, '{}.zip'.format(scene_id))
            extracted_file_path = os.path.join(product_dir, '{}.SAFE'.format(scene_id))

            collection_item.compressed_file = zip_file_name
            
            try:
                valid = True

                if os.path.exists(zip_file_name):
                    logging.debug('zip file exists')
                    valid = is_valid(zip_file_name)

                if not os.path.exists(zip_file_name) or not valid:
                    # Download from Copernicus
                    download_sentinel_images(link, zip_file_name, user)

                    # Check if file is valid
                    valid = is_valid(zip_file_name)

                if not valid:
                    raise IOError('Invalid zip file "{}"'.format(zip_file_name))
                else:
                    extractall(zip_file_name)
                logging.debug('Done download.')
                activity_history.activity.status = 'DONE'
                activity_history.activity.file = extracted_file_path

            except ConnectionError as e:
                logging.error('Connection error', e)
                activity_history.activity.status = 'ERROR'
                if os.path.exists(zip_file_name):
                    os.remove(zip_file_name)
                raise e

            except BaseException as e:
                logging.error('An error occurred during task execution', e)
                activity_history.activity.status = 'ERROR'

                raise e
            finally:
                activity_history.end = datetime.utcnow()
                activity_history.save()

        scene.update(dict(
            file=extracted_file_path
        ))

        # Create new activity 'correctionS2' to continue task chain
        scene['app'] = 'correctionS2'

        return scene

    def correction(self, scene):
        logging.debug('Starting Correction Sentinel...')
        version = 'sen2cor280'

        activity_history = get_task_activity()
        activity_history.activity.status = 'DOING'
        activity_history.start = datetime.utcnow()
        activity_history.save()

        try:
            if version == 'sen2cor280':
                correction_result = correction_sen2cor280( scene )
            else:
                correction_result = correction_sen2cor255( scene )
            if correction_result is not None:
                scene['file'] = correction_result
            activity_history.activity.status = 'DONE'

        except BaseException as e:
            logging.error('An error occurred during task execution', e)
            activity_history.activity.status = 'ERROR'
            raise e
        finally:
            activity_history.end = datetime.utcnow()
            activity_history.save()

        scene['app'] = 'publishS2'

        return scene

    def publish(self, scene):
        #TODO: check if is already published before publishing
        logging.debug('Starting Publish Sentinel...')

        activity_history = get_task_activity()
        activity_history.activity.status = 'DOING'
        activity_history.start = datetime.utcnow()
        activity_history.save()

        try:
            publish(activity_history.activity)
            activity_history.activity.status = 'DONE'
        except BaseException as e:
            logging.error('An error occurred during task execution', e)
            activity_history.activity.status = 'ERROR'
            raise e
        finally:
            activity_history.end = datetime.utcnow()
            activity_history.save()

        # Create new activity 'uploadS2' to continue task chain
        scene['app'] = 'uploadS2'

        logging.debug('Done Publish Sentinel.')

        return scene

    def upload(self, scene):
        activity_history = get_task_activity()
        activity_history.activity.status = 'DONE'
        activity_history.start = datetime.utcnow()
        activity_history.end = datetime.utcnow()
        activity_history.save()


# TODO: Sometimes, copernicus reject the connection even using only 2 concurrent connection
# We should set "autoretry_for" and retry_kwargs={'max_retries': 3} to retry
# task execution since it seems to be bug related to the api
@celery_app.task(base=SentinelTask, queue='download')
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


@celery_app.task(base=SentinelTask, queue='atm-correction')
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


@celery_app.task(base=SentinelTask, queue='publish')
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


@celery_app.task(base=SentinelTask, queue='upload')
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
