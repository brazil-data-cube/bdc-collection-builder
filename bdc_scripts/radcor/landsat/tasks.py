# Python Native
import logging
import os

# BDC Scripts
from bdc_scripts.config import Config
from bdc_scripts.celery import celery_app
from bdc_scripts.radcor.landsat.download import download_landsat_images
from bdc_scripts.radcor.utils import get_task_activity


class LandsatTask(celery_app.Task):
    def download(self, scene):
        activity = get_task_activity()
        activity.status = 'DOING'
        activity.save()

        cc = scene['sceneid'].split('_')
        pathrow = cc[2]
        yyyymm = cc[3][:4]+'-'+cc[3][4:6]
        # Output product dir
        productdir = os.path.join(Config.DATA_DIR, 'LC8/{}/{}'.format(yyyymm,pathrow))

        if not os.path.exists(productdir):
            os.makedirs(productdir)

        link = scene['link']

        file = download_landsat_images(link, productdir)



    def publish(self, scene):
        pass

    def upload(self, scene):
        pass


@celery_app.task(base=LandsatTask, queue='download')
def download_landsat(scene):
    return download_landsat.download(scene)


@celery_app.task(base=LandsatTask, queue='publish')
def publish_landsat(scene):
    return publish_landsat.publish(scene)


@celery_app.task(base=LandsatTask, queue='upload')
def upload_landsat(scene):
    upload_landsat.upload(scene)
