# 3rdparty
from flask import request
from flask_restplus import Namespace, Resource
from werkzeug.exceptions import BadRequest

# BDC Scripts
from .business import CubeBusiness
from .parsers import DataCubeParser, DataCubeProcessParser


api = Namespace('datastorm', description='datastorm')


@api.route('/create')
class CubeCreateController(Resource):
    def post(self):
        form = DataCubeParser()

        args = request.get_json()

        errors = form.validate(args)

        if errors:
            return errors, 400

        data = form.load(args)

        cubes, status = CubeBusiness.create(data)

        return cubes, status


@api.route('/process')
class CubeProcessController(Resource):
    def post(self):
        args = request.get_json()

        form = DataCubeProcessParser()

        data = form.load(args)

        proc = CubeBusiness.process(data['datacube'], data['collections'], data['tiles'], data['start_date'], data['end_date'])

        return proc
