# 3rdparty
from flask import request
from flask_restplus import Namespace, Resource
from werkzeug.exceptions import BadRequest

# BDC Scripts
from bdc_scripts.radcor.forms import RadcorActivityForm
from bdc_scripts.radcor.models import RadcorActivity
from bdc_scripts.radcor.business import RadcorBusiness
from bdc_scripts.radcor.sentinel import tasks


api = Namespace('radcor', description='radcor')


@api.route('/')
class RadcorController(Resource):
    def get(self):
        # curl localhost:5000/api/radcor?w=-45.90\&s=-12.74\&n=-12.6\&e=-45.80\&satsen=S2\&start=2019-01-01\&end=2019-01-15\&cloud=90\&action=qualquercoisa

        args = request.args.to_dict()

        if 'w' not in args or \
           'n' not in args or \
           'e' not in args or \
           's' not in args:
           raise BadRequest('Datacube or Bounding Box must be given')


        # Prepare radcor activity and start
        result = RadcorBusiness.radcor(args)

        if 'LC8' in args.get('satsen') or 'LC8SR' in args.get('satsen'):
            result = filter(result,tags=['cloud','date','status'])
        else:
            result = filter(result)

        tile = '{}-{}-{}'.format(args['tileid'], args['start'], args['end'])

        scenes = {
            tile: RadcorActivityForm().dump(result),
            'Results': len(result)
        }

        return scenes


@api.route('/restart')
class RadcorRestartController(Resource):
    def get(self):
        activities = RadcorActivity.reset_status(id=request.args.get('id'))

        # Dispatch to the celery
        for activity in activities:
            RadcorBusiness.start(activity)

        return dict()