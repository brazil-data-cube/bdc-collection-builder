"""
Brazil Data Cube Scripts Blueprint strategy
"""

from flask import Blueprint
from flask_restplus import Api
from bdc_collection_builder.collections.controller import api as radcor_ns


bp = Blueprint('bdc_collection_builder', __name__, url_prefix='/api')

api = Api(bp, doc=False)

api.add_namespace(radcor_ns)