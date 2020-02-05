# 3rdparty
from flask import request
from flask_restplus import Namespace, Resource
from werkzeug.exceptions import BadRequest, NotFound
from bdc_core.decorators.auth import require_oauth_scopes
from bdc_db.models.collection import Collection

# Builder
from bdc_collection_builder.celery.utils import list_pending_tasks, list_running_tasks
from .forms import RadcorActivityForm
from .models import RadcorActivity
from .business import RadcorBusiness

import requests

api = Namespace('radcor', description='radcor')


@api.route('/')
class RadcorController(Resource):
    @require_oauth_scopes(scope="collection_builder:activities:GET")
    def get(self):
        """Retrieves all radcor activities from database"""

        args = request.args
        page = int(args.get('page', 1))
        per_page = int(args.get('per_page', 10))

        activities = RadcorBusiness.list_activities(args)\
            .paginate(page, per_page)

        return {
            "total": activities.total,
            "page": activities.page,
            "per_page": activities.per_page,
            "pages": activities.pages,
            "items": RadcorActivityForm().dump(activities.items, many=True)
        }

    @require_oauth_scopes(scope="collection_builder:activities:POST")
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
    @require_oauth_scopes(scope="collection_builder:activities:POST")
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
    @require_oauth_scopes(scope="collection_builder:activities:GET")
    def get(self):
        return list_running_tasks()


@api.route('/stats/pending')
class RadcorPendingTasksController(Resource):
    @require_oauth_scopes(scope="collection_builder:activities:GET")
    def get(self):
        return list_pending_tasks()


@api.route('/utils/collections-available')
class RadcorCollectionsController(Resource):
    @require_oauth_scopes(scope="collection_builder:activities:GET")
    def get(self):
        return {
            'collections': RadcorBusiness.get_collections_activities()
        }


@api.route('/utils/count-activities')
class RadcorCollectionsController(Resource):
    @require_oauth_scopes(scope="collection_builder:activities:GET")
    def get(self):
        args = request.args

        result = RadcorBusiness.count_activities(args)
        return result


@api.route('/utils/count-activities-date')
class RadcorCollectionsController(Resource):
    @require_oauth_scopes(scope="collection_builder:activities:GET")
    def get(self):
        args = request.args

        result = RadcorBusiness.count_activities_with_date(args)
        return result

@api.route('/utils/count-unsuccessfully-activities')
class RadcorCollectionsController(Resource):
    @require_oauth_scopes(scope="collection_builder:activities:GET")
    def get(self):
        result = RadcorBusiness.get_unsuccessfully_activities()
        return result