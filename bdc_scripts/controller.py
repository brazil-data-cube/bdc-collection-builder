from flask import request
from flask_restplus import Namespace, Resource
from bdc_scripts.tasks import download_sentinel, publish_sentinel, upload_sentinel
from celery import chain


ns = Namespace('sentinel', description='sentinel')


@ns.route('/download')
class DownloadSentinelController(Resource):
    def get(self):
        number = int(request.args.get('size', 20))

        for i in range(number):
            download_sentinel.delay()

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
