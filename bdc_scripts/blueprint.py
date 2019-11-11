"""
Brazil Data Cube Scripts Blueprint strategy
"""

from flask import Blueprint
from flask_restplus import Api
from bdc_scripts.controller import ns as sample_ns
from bdc_scripts.radcor.controller import api as radcor_ns


bp = Blueprint('bdc-scripts', __name__, url_prefix='/api')

api = Api(bp, doc=False)

api.add_namespace(sample_ns)
api.add_namespace(radcor_ns)