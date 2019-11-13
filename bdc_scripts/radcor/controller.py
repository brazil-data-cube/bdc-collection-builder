# 3rdparty
from flask import request
from flask_restplus import Namespace, Resource
from werkzeug.exceptions import BadRequest

# BDC Scripts
from bdc_scripts.radcor.forms import RadcorActivityForm
from bdc_scripts.radcor.models import RadcorActivity
from bdc_scripts.radcor.business import RadcorBusiness


api = Namespace('radcor', description='radcor')


@api.route('/')
class RadcorController(Resource):
    def get(self):
        """Retrieves all radcor activities from database"""

        activities = RadcorActivity.filter()

        return RadcorActivityForm().dump(activities, many=True)

    def post(self):
        """
        curl -XPOST -H "Content-Type: application/json" \
            --data '{"w": -45.90, "s": -12.74, "n": -12.6, "e": -45.80, "satsen": "S2", "start": "2019-01-01", "end": "2019-01-15", "cloud": 90, "action": "start"}' \
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

        # if 'LC8' in args.get('satsen') or 'LC8SR' in args.get('satsen'):
        #     result = filter(result,tags=['cloud','date','status'])
        # else:
        #     result = filter(result)

        tile = '{}-{}-{}'.format(args['tileid'], args['start'], args['end'])

        scenes = {
            tile: result,
            'Results': len(result)
        }

        return scenes


@api.route('/restart')
class RadcorRestartController(Resource):
    def get(self):
        RadcorBusiness.restart(id=request.args.get('id'))

        return dict()