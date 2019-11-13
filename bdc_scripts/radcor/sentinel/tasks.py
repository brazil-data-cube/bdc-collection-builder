# Python Native
import logging
import os
import time
from random import randint

# BDC Scripts
from bdc_scripts.celery import celery_app
from bdc_scripts.celery.cache import lock_handler
from bdc_scripts.core.utils import extractall, is_valid
from bdc_scripts.radcor.sentinel.clients import sentinel_clients
from bdc_scripts.radcor.sentinel.download import download_sentinel_images
from bdc_scripts.radcor.sentinel.publish import publish
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
            logging.debug('Resource locked....')
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

        # Persist the activity to done
        activity = get_task_activity()

        activity.status = 'DOING'
        activity.save()

        # Acquire User to download
        with self.get_user() as user:
            logging.debug('Starting Download {}...'.format(user.username))

            try:
                cc = scene['sceneid'].split('_')
                year_month = cc[2][:4] + '-' + cc[2][4:6]

                # Output product dir
                product_dir = os.path.join(scene.get('file'), year_month)
                link = scene['link']
                scene_id = scene['sceneid']

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
                activity.status = 'DONE'
            except BaseException as e:
                logging.error('An error occurred during task execution', e)
                activity.status = 'ERROR'

                raise e
            finally:
                activity.save()

        # TODO: Add atmospheric correction (sen2cor, espa)

        scene.update(dict(
            file=extracted_file_path
        ))

        # Create new activity 'publish' to continue task chain
        scene['app'] = 'publishS2'

        return scene

    def publish(self, scene):
        logging.debug('Starting Publish Sentinel...')

        activity = get_task_activity()
        activity.status = 'DOING'
        activity.save()

        try:
            publish(activity)
            activity.status = 'DONE'
        except BaseException as e:
            logging.error('An error occurred during task execution', e)
            activity.status = 'ERROR'
            raise e
        finally:
            activity.save()

        # Create new activity 'publish' to continue task chain
        scene['app'] = 'uploadS2'

        logging.debug('Done Publish Sentinel.')

        return scene

    def upload(self, scene):
        logging.debug('Starting Upload sentinel to AWS...')

        time.sleep(randint(4, 8))

        logging.debug('Done Upload sentinel to AWS.')


# TODO: Sometimes, copernicus reject the connection even using only 2 concurrent connection
# We should set "autoretry_for" and retry_kwargs={'max_retries': 3} to retry
# task execution since it seems to be bug related to the api
@celery_app.task(base=SentinelTask, queue='download')
def download_sentinel(scene):
    return download_sentinel.download(scene)


@celery_app.task(base=SentinelTask, queue='publish')
def publish_sentinel(scene):
    return publish_sentinel.publish(scene)


@celery_app.task(base=SentinelTask, queue='upload')
def upload_sentinel(scene):
    upload_sentinel.upload(scene)