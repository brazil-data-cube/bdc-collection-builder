# Python Native
import logging
import os
import time

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
        activity = get_task_activity()
        activity.status = 'DOING'
        activity.save()

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

        except BaseException as e:
            logging.error('An error occurred during task execution', e)
            activity.status = 'ERROR'

            raise e
        finally:
            activity.save()

        scene.update(dict(
            file=file
        ))

        scene['app'] = 'correctionLC8'

        return scene

    def publish(self, scene):
        activity = get_task_activity()
        activity.status = 'DOING'
        activity.save()

        try:
            publish(scene)
        except BaseException as e:
            logging.error('An error occurred during task execution', e)
            activity.status = 'ERROR'

            raise e
        finally:
            activity.save()

        return scene

    def upload(self, scene):
        pass

    @staticmethod
    def espa_done(productdir, pathrow, date):
        template = os.path.join(productdir, 'LC08_*_{}_{}_*.tif'.format(pathrow, date))

        fs = resource_glob(template)

        return len(fs) > 0

    def correction(self, scene):
        activity = get_task_activity()
        activity.status = 'DOING'
        activity.save()

        # Send scene to the ESPA service
        req = resource_get('{}/espa'.format(Config.ESPA_URL), params=scene)
        # Ensure the request has been successfully
        assert req.status_code == 200

        identifier = scene['sceneid']
        cc = scene['sceneid'].split('_')
        pathrow = cc[2]
        date = cc[3]
        yyyymm = cc[3][:4]+'-'+cc[3][4:6]
        # Product dir
        productdir = scene.get('file')
        
        while not LandsatTask.espa_done(productdir, pathrow, date):
            logging.debug('Atmospheric correction is not done yet...')
            time.sleep(5)

        scene['app'] = 'publishLC8'

        return scene


@celery_app.task(base=LandsatTask, queue='download')
def download_landsat(scene):
    return download_landsat.download(scene)


@celery_app.task(base=LandsatTask, queue='atm-correction')
def amt_correction_landsat(scene):
    return amt_correction_landsat.correction(scene)


@celery_app.task(base=LandsatTask, queue='publish')
def publish_landsat(scene):
    return publish_landsat.publish(scene)


@celery_app.task(base=LandsatTask, queue='upload')
def upload_landsat(scene):
    upload_landsat.upload(scene)
