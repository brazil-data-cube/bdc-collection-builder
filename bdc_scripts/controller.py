from flask_restplus import Namespace, Resource
from bdc_scripts.tasks import download_sentinel, publish_sentinel, upload_sentinel
from celery import chain


ns = Namespace('sentinel', description='sentinel')


@ns.route('/download')
class DownloadSentinelController(Resource):
    def get(self):
        number = 20
        tasks = []
        for i in range(number):
            tasks.append(download_sentinel.s())

        c = chain(*tasks)
        c.apply_async()

        return {"status": 200, "triggered": number}


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
