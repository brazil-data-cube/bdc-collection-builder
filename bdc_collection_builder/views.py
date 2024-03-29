#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2022 INPE.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/gpl-3.0.html>.
#

"""Define flask views for collections."""

# 3rdparty
from flask import Blueprint, jsonify, request
from werkzeug.exceptions import RequestURITooLarge

# Builder
from .celery.utils import list_pending_tasks, list_running_tasks
from .controller import RadcorBusiness
from .forms import CheckScenesForm, RadcorActivityForm, SearchImageForm

bp = Blueprint('radcor', import_name=__name__, url_prefix='/api')


@bp.route('/activities', methods=('GET',))
def list_activities():
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


@bp.route('/radcor', methods=('POST', ))
def dispatch_collector():
    """Dispatch task execution of collection.

    curl -XPOST -H "Content-Type: application/json" \
        --data '{"w": -46.40, "s": -13.1, "n": -13, "e": -46.3, "satsen": "S2", "start": "2019-01-01", "end": "2019-01-30", "cloud": 90, "action": "start"}' \
        localhost:5000/api/radcor/
    """
    args = request.get_json()

    form = SearchImageForm()

    errors = form.validate(args)

    if errors:
        return errors

    data = form.load(args)

    # Prepare radcor activity and start
    result = RadcorBusiness.radcor(data)

    scenes = {
        'tiles': result,
        'Results': len(result)
    }

    return scenes


def _restart(args: dict):
    """Restart celery task execution.

    It supports the following parameters:
        - id : Restart activity by id
        - ids : Restart a list of activity by ids
        - activity_type : Restart all activities
        - sceneid : A sceneid or list to filter. You must provide activity_type and collection_id
        - collection_id : Collection to filter
    """
    if 'id' in args:
        args['ids'] = str(args.pop('id'))

    if 'ids' in args:
        args['ids'] = args['ids'].split(',') if isinstance(args['ids'], str) else args['ids']

    args.setdefault('action', None)
    args.setdefault('use_aws', False)

    activities = RadcorBusiness.restart(**args)

    return dict(
        action='PREVIEW' if args['action'] is None else args['action'],
        total=len(activities),
        activities=activities
    )


@bp.route('/radcor/restart', methods=('GET', 'POST',))
def restart():
    """Restart Task.

    The request is limited to 4Kb.

    curl "localhost:5000/api/radcor/restart?ids=13,17&action=start"
    """
    if request.method == 'POST':
        args = request.get_json()
        return _restart(args)

    # Limit request query string to 4KB on GET
    if len(request.query_string) > 4096:
        raise RequestURITooLarge('Query is too long. Use the method POST instead.')

    args = request.args.to_dict()

    return _restart(args)


@bp.route('/stats/active')
def running_tasks():
    """Retrieve running tasks on workers."""
    return list_running_tasks()


@bp.route('/stats/pending')
def pending_tasks():
    """List pending tasks on workers."""
    return list_pending_tasks()


@bp.route('/utils/collections-available')
def list_distinct_activities():
    """List distinct activities."""
    return {
        'collections': RadcorBusiness.get_collections_activities()
    }


@bp.route('/utils/count-activities')
def count_activities():
    """List total activities."""
    args = request.args

    result = RadcorBusiness.count_activities(args)
    return result


@bp.route('/utils/count-activities-date')
def count_activities_by_date():
    """List activities grouped by date."""
    args = request.args

    result = RadcorBusiness.count_activities_with_date(args)
    return result


@bp.route('/utils/count-unsuccessfully-activities')
def count_failed_activities():
    """List count of failed tasks."""
    result = RadcorBusiness.get_unsuccessfully_activities()
    return result


@bp.route('/check-scenes', methods=('POST',))
def check_scenes():
    """Check for scene availability in collection builder."""
    data = request.get_json()

    form = CheckScenesForm()

    errors = form.validate(data)

    if errors:
        return errors, 400

    data = form.load(data)

    result = RadcorBusiness.check_scenes(**data)

    return result, 200


@bp.route('/collections', methods=('GET', ))
def list_collections():
    """Retrieve the BDC Collections (including cubes) from database."""
    collections = RadcorBusiness.list_collections()

    return jsonify(collections), 200


@bp.route('/collections/<int:collection_id>/tiles', methods=('GET', ))
def list_collection_tiles(collection_id: int):
    """Retrieve all collected tiles related with Collection."""
    resp = RadcorBusiness.list_collection_tiles(collection_id)

    return jsonify(resp), 200


@bp.route('/grids', defaults=dict(grid_id=None), methods=('GET', ))
@bp.route('/grids/<int:grid_id>', methods=('GET', ))
def list_grids(grid_id: int = None):
    """List the Grid definition of a BDC Collection."""
    bbox = None

    if request.args.get('bbox'):
        bbox = request.args['bbox'].split(',')

    resp = RadcorBusiness.list_grids(grid_id, bbox=bbox)

    return jsonify(resp), 200


@bp.route('/providers', methods=('GET', ))
def list_providers():
    """Retrieve all available data providers on Collection Builder."""
    catalogs = RadcorBusiness.list_catalogs()

    return jsonify(catalogs), 200
