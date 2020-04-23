#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Define flask views for collections."""


# 3rdparty
from flask import request
from flask_restplus import Namespace, Resource
from werkzeug.exceptions import BadRequest
# Builder
from bdc_collection_builder.celery.utils import list_pending_tasks, list_running_tasks
from .forms import RadcorActivityForm
from .business import RadcorBusiness


api = Namespace('radcor', description='radcor')


@api.route('/')
class RadcorController(Resource):
    """Define controller to dispatch activity and celery execution."""

    def get(self):
        """Retrieve all radcor activities from database."""
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

    def post(self):
        """Dispatch task execution of collection.

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
    """Define flask controller resource for Restart tasks.

    This route requires OAuth2 token to work properly.
    """

    def get(self):
        """Restart Task."""
        args = request.args.to_dict()

        if 'id' in args:
            args['ids'] = args['id']

        if 'ids' in args:
            args['ids'] = args['ids'].split(',')

        RadcorBusiness.restart(**args)

        return dict()


@api.route('/stats/active')
class RadcorActiveTasksController(Resource):
    """Define flask resource to communicate realtime with celery workers.

    List active tasks on celery worker.
    """

    def get(self):
        """Retrieve running tasks on workers."""
        return list_running_tasks()


@api.route('/stats/pending')
class RadcorPendingTasksController(Resource):
    """Define flask resource to communicate realtime with celery workers.

    List pending tasks on celery worker.
    """

    def get(self):
        """List pending tasks on workers."""
        return list_pending_tasks()


@api.route('/utils/collections-available')
class RadcorCollectionsController(Resource):
    """Define flask resource to list distinct activities based on history."""

    def get(self):
        """List distinct activities."""
        return {
            'collections': RadcorBusiness.get_collections_activities()
        }


@api.route('/utils/count-activities')
class RadcorCollectionsController(Resource):
    """Define flask resource to count activities."""

    def get(self):
        """List total activities."""
        args = request.args

        result = RadcorBusiness.count_activities(args)
        return result


@api.route('/utils/count-activities-date')
class RadcorCollectionsController(Resource):
    """Define flask resource to count all activities by date."""

    def get(self):
        """List activities grouped by date."""
        args = request.args

        result = RadcorBusiness.count_activities_with_date(args)
        return result


@api.route('/utils/count-unsuccessfully-activities')
class RadcorCollectionsController(Resource):
    """Define flask resource to count all failed tasks."""

    def get(self):
        """List count of failed tasks."""
        result = RadcorBusiness.get_unsuccessfully_activities()
        return result
