#
# This file is part of BDC Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# BDC Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Define Brazil Data Cube Scripts Blueprint strategy."""

from flask import Blueprint
from flask_restplus import Api

from .collections.controller import api as radcor_ns

bp = Blueprint('bdc_collection_builder', __name__, url_prefix='/api')

api = Api(bp, doc=False)

api.add_namespace(radcor_ns)
