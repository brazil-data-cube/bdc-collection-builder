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
from bdc_scripts.radcor.landsat.download import download_landsat_images
from bdc_scripts.radcor.landsat.publish import publish
from bdc_scripts.radcor.utils import get_task_activity


class LandsatTask(celery_app.Task):
    def download(self, scene):
        activity_history = get_task_activity()
        activity_history.activity.status = 'DOING'
        activity_history.start = datetime.utcnow()
        activity_history.save()

        try:
            cc = scene['sceneid'].split('_')
            pathrow = cc[2]
            yyyymm = cc[3][:4]+'-'+cc[3][4:6]

            # Output product dir
            productdir = os.path.join(scene.get('file'), '{}/{}'.format(yyyymm, pathrow))

            if not os.path.exists(productdir):
                os.makedirs(productdir)

            link = scene['link']

            file = download_landsat_images(link, productdir)
            activity_history.status = 'DONE'

        except BaseException as e:
            logging.error('An error occurred during task execution', e)
            activity_history.status = 'ERROR'

            raise e
        finally:
            activity_history.end = datetime.utcnow()
            activity_history.save()

        scene.update(dict(
            file=file
        ))

        scene['app'] = 'correctionLC8'

        return scene

    def publish(self, scene):
        activity_history = get_task_activity()
        activity_history.activity.status = 'DOING'
        activity_history.start = datetime.utcnow()
        activity_history.save()

        try:
            publish(scene)
            activity_history.status = 'DONE'
        except BaseException as e:
            logging.error('An error occurred during task execution', e)
            activity_history.status = 'ERROR'

            raise e
        finally:
            activity_history.end = datetime.utcnow()
            activity_history.save()

        scene['app'] = 'uploadLC8'

        return scene

    def upload(self, scene):
        activity = get_task_activity()
        activity.status = 'DONE'
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
            # Send scene to the ESPA service
            req = resource_get('{}/espa'.format(Config.ESPA_URL), params=scene)
            # Ensure the request has been successfully
            assert req.status_code == 200

            cc = scene['sceneid'].split('_')
            pathrow = cc[2]
            date = cc[3]
            yyyymm = cc[3][:4]+'-'+cc[3][4:6]
            # Product dir
            productdir = os.path.join(Config.DATA_DIR, 'Repository/Archive/LC8SR/{}/{}'.format(yyyymm, pathrow))

            logging.info('Checking for the ESPA generated files in {}'.format(productdir))

            while not LandsatTask.espa_done(productdir, pathrow, date):
                logging.debug('Atmospheric correction is not done yet...')
                time.sleep(5)

            activity_history.status = 'DONE'

            scene['file'] = productdir

        except BaseException as e:
            logging.error('Error at ATM correction Landsat', e)
            activity_history.status = 'ERROR'

            raise e
        finally:
            activity_history.end = datetime.utcnow()
            activity_history.save()

        scene['app'] = 'publishLC8'

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
