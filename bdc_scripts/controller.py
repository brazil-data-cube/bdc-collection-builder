import os
import tempfile
from celery import chain, current_app
from flask_restplus import Namespace, Resource
from bdc_scripts.celery import app as celery_app
from bdc_scripts.celery.tasks import download_sentinel, publish_sentinel, upload_sentinel


ns = Namespace('sentinel', description='sentinel')


DESTINATION_DIR = os.path.join(tempfile.gettempdir(), 'celery-bdc-scripts')

scenes = [
    dict(
        scene_id='S2A_MSIL2A_20190105T132231_N0211_R038_T23LLG_20190105T153844',
        link='https://scihub.copernicus.eu/apihub/odata/v1/Products(\'59f69fbe-5bcd-4117-8de8-fa97b6d203c7\')/$value',
        destination=DESTINATION_DIR
    ),
    dict(
        scene_id='S2A_MSIL2A_20190105T132231_N0211_R038_T23LMF_20190105T153844',
        link='https://scihub.copernicus.eu/apihub/odata/v1/Products(\'d0604a59-493f-4252-bbdc-78e2f6cacd1b\')/$value',
        destination=DESTINATION_DIR
    ),
    dict(
        scene_id='S2A_MSIL2A_20190105T132231_N0211_R038_T23LMG_20190105T153844',
        link='https://scihub.copernicus.eu/apihub/odata/v1/Products(\'129bf1f7-cbe5-4912-a693-3f07d80c57ac\')/$value',
        destination=DESTINATION_DIR
    ),
    dict(
        scene_id='S2A_MSIL2A_20190115T132231_N0211_R038_T23LLG_20190115T153630',
        link='https://scihub.copernicus.eu/apihub/odata/v1/Products(\'b651037f-3ce2-45c6-accf-0c15ba7f997a\')/$value',
        destination=DESTINATION_DIR
    ),
    dict(
        scene_id='S2A_MSIL2A_20190115T132231_N0211_R038_T23LMF_20190115T153630',
        link='https://scihub.copernicus.eu/apihub/odata/v1/Products(\'f835aca8-c605-4b6c-8556-3a21c4c4aa76\')/$value',
        destination=DESTINATION_DIR
    ),
    dict(
        scene_id='S2A_MSIL2A_20190115T132231_N0211_R038_T23LMG_20190115T153630',
        link='https://scihub.copernicus.eu/apihub/odata/v1/Products(\'b7d1d43b-02c2-41de-a04f-89ebdc47612c\')/$value',
        destination=DESTINATION_DIR
    ),
]

@ns.route('/tasks')
class ListTasks(Resource):
    def get(self):
        inspector = current_app.control.inspect()

        return inspector.active()

@ns.route('/download')
class DownloadSentinelController(Resource):
    def get(self):
        for scene in scenes:
            download_sentinel.delay(scene)

        return {"status": 200, "triggered": len(scenes)}


@ns.route('/download+publish')
class DownloadSentinelController(Resource):
    def get(self):
        for scene in scenes:
            tasks = download_sentinel.s(scene) | publish_sentinel.s()

            chain(tasks).apply_async()

        return {"status": 200, "triggered": len(scenes)}

@ns.route('/publish')
class PublishSentinelController(Resource):
    def get(self):
        number = 5
        for i in range(number):
            publish_sentinel.s()

        return {"status": 200, "triggered": number}


@ns.route('/upload')
class UploadSentinelController(Resource):
    def get(self):
        number = 5
        for i in range(number):
            upload_sentinel.s()

        return {"status": 200, "triggered": number}
