# 3rdparty
from flask import request
from flask_restplus import Namespace, Resource
from werkzeug.exceptions import BadRequest, NotFound

# BDC Scripts
from bdc_db.models.collection import Collection
from bdc_scripts.celery.utils import list_pending_tasks, list_running_tasks
from bdc_scripts.radcor.forms import RadcorActivityForm
from bdc_scripts.radcor.models import RadcorActivity
from bdc_scripts.radcor.business import RadcorBusiness

import requests

api = Namespace('radcor', description='radcor')


@api.route('/')
class RadcorController(Resource):
    def get(self):
        """Retrieves all radcor activities from database"""

        activities = RadcorActivity.query().all()

        return RadcorActivityForm().dump(activities, many=True)

    def post(self):
        """
        curl -XPOST -H "Content-Type: application/json" \
            --data '{"w": -46.40, "s": -13.1, "n": -13, "e": -46.3, "satsen": "S2", "start": "2019-01-01", "end": "2019-01-30", "cloud": 90, "action": "start"}' \
            localhost:5000/api/radcor/
        """

        args = request.get_json()

        if 'w' not in args or \
                'n' not in args or \
                'e' not in args or \
                's' not in args:
            raise BadRequest('Datacube or Bounding Box must be given')

        # Prepare radcor activity and start
        result = RadcorBusiness.radcor(args)

        tile = '{}-{}-{}'.format(args['tileid'], args['start'], args['end'])

        scenes = {
            tile: result,
            'Results': len(result)
        }

        return scenes


@api.route('/restart')
class RadcorRestartController(Resource):
    def get(self):
        args = request.args.to_dict()

        if 'id' in args:
            args['ids'] = args['id']

        if 'ids' in args:
            args['ids'] = args['ids'].split(',')

        RadcorBusiness.restart(**args)

        return dict()


@api.route('/stats/active')
class RadcorActiveTasksController(Resource):
    def get(self):
        return list_running_tasks()


@api.route('/stats/pending')
class RadcorPendingTasksController(Resource):
    def get(self):
        return list_pending_tasks()