# Python Native
import logging
import os
import time
from datetime import datetime

# 3rdparty
from glob import glob as resource_glob
from requests import get as resource_get

# BDC Scripts
from bdc_scripts.celery import celery_app
from bdc_scripts.config import Config
from bdc_scripts.radcor.base_task import RadcorTask
from bdc_scripts.radcor.landsat.download import download_landsat_images
from bdc_scripts.radcor.landsat.publish import publish
from bdc_scripts.radcor.utils import get_task_activity


class LandsatTask(RadcorTask):
    def get_tile_id(self, scene_id, **kwargs):
        fragments = scene_id.split('_')
        return fragments[2]

    def get_tile_date(self, scene_id, **kwargs):
        fragments = scene_id.split('_')

        return datetime.strptime(fragments[3], '%Y%m%d')

    def download(self, scene):
        activity_history = get_task_activity()
        activity_history.activity.status = 'DOING'
        activity_history.start = datetime.utcnow()
        activity_history.save()

        try:
            scene_id = scene['sceneid']
            yyyymm = self.get_tile_date(scene_id).strftime('%Y-%m')
            activity_args = scene.get('args', {})

            collection_item = self.get_collection_item(activity_history.activity)

            # Output product dir
            productdir = os.path.join(activity_args.get('file'), '{}/{}'.format(yyyymm, self.get_tile_id(scene_id)))

            if not os.path.exists(productdir):
                os.makedirs(productdir)

            file = download_landsat_images(activity_args['link'], productdir)

            collection_item.compressed_file = file

            cloud = activity_args.get('cloud')

            if cloud:
                collection_item.cloud_cover = cloud

            activity_args['file'] = file

            activity_history.activity.status = 'DONE'

        except BaseException as e:
            logging.error('An error occurred during task execution', e)
            activity_history.activity.status = 'ERROR'

            raise e
        finally:
            activity_history.end = datetime.utcnow()
            activity_history.save()

        collection_item.save()

        scene['args'] = activity_args

        # Create new activity 'correctionS2' to continue task chain
        scene['activity_type'] = 'correctionLC8'

        return scene

    def publish(self, scene):
        activity_history = get_task_activity()
        activity_history.activity.status = 'DOING'
        activity_history.start = datetime.utcnow()
        activity_history.save()

        try:
            publish(scene)
            activity_history.activity.status = 'DONE'
        except BaseException as e:
            logging.error('An error occurred during task execution', e)
            activity_history.activity.status = 'ERROR'

            raise e
        finally:
            activity_history.end = datetime.utcnow()
            activity_history.save()

        scene['app'] = 'uploadLC8'

        return scene

    def upload(self, scene):
        activity = get_task_activity()
        activity.activity.status = 'DONE'
        activity.save()

    @staticmethod
    def espa_done(productdir, pathrow, date):
        template = os.path.join(productdir, 'LC08_*_{}_{}_*.tif'.format(pathrow, date))

        fs = resource_glob(template)

        return len(fs) > 0

    def correction(self, scene):
        activity_history = get_task_activity()
        activity_history.activity.status = 'DOING'
        activity_history.start = datetime.utcnow()
        activity_history.save()

        try:
            params = dict(
                app=scene['activity_type'],
                sceneid=scene['sceneid'],
                file=scene['args']['file']
            )

            # Send scene to the ESPA service
            req = resource_get('{}/espa'.format(Config.ESPA_URL), params=params)
            # Ensure the request has been successfully
            assert req.status_code == 200

            scene_id = scene['sceneid']
            pathrow = self.get_tile_id(scene_id)
            tile_date = self.get_tile_date(scene_id)
            yyyymm = tile_date.strftime('%Y-%m')
            date = tile_date.strftime('%Y-%m-d')

            # Product dir
            productdir = os.path.join(Config.DATA_DIR, 'Repository/Archive/{}/{}/{}'.format(scene['collection_id'], yyyymm, pathrow))

            logging.info('Checking for the ESPA generated files in {}'.format(productdir))

            while not LandsatTask.espa_done(productdir, pathrow, date):
                logging.debug('Atmospheric correction is not done yet...')
                time.sleep(5)

            activity_history.activity.status = 'DONE'

            scene['args']['file'] = productdir

        except BaseException as e:
            logging.error('Error at ATM correction Landsat', e)
            activity_history.activity.status = 'ERROR'

            raise e
        finally:
            activity_history.end = datetime.utcnow()
            activity_history.save()

        scene['activity_type'] = 'publishLC8'

        return scene


@celery_app.task(base=LandsatTask, queue='download')
def download_landsat(scene):
    return download_landsat.download(scene)


@celery_app.task(base=LandsatTask, queue='atm-correction')
def atm_correction_landsat(scene):
    return atm_correction_landsat.correction(scene)


@celery_app.task(base=LandsatTask, queue='publish')
def publish_landsat(scene):
    return publish_landsat.publish(scene)


@celery_app.task(base=LandsatTask, queue='upload')
def upload_landsat(scene):
    upload_landsat.upload(scene)
