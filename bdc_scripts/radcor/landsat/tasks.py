# Python Native
import logging
import os

# BDC Scripts
from bdc_scripts.celery import celery_app
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


@celery_app.task(base=LandsatTask, queue='download')
def download_landsat(scene):
    return download_landsat.download(scene)


@celery_app.task(base=LandsatTask, queue='publish')
def publish_landsat(scene):
    return publish_landsat.publish(scene)


@celery_app.task(base=LandsatTask, queue='upload')
def upload_landsat(scene):
    upload_landsat.upload(scene)
