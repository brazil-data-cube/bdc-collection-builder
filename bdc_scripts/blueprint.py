"""
Brazil Data Cube Scripts Blueprint strategy
"""

from flask import Blueprint
from flask_restplus import Api
from bdc_scripts.controller import ns as sample_ns


bp = Blueprint('sample', __name__, url_prefix='/api')

api = Api(bp, doc=False)

api.add_namespace(sample_ns)