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

"""Python Brazil Data Cube Collection Builder."""

from json import JSONEncoder

import redis
from bdc_catalog.ext import BDCCatalog
from flask import Flask
from werkzeug.exceptions import HTTPException, InternalServerError

from . import celery, config
from .celery.utils import load_celery_models
from .config import get_settings
from .version import __version__


def create_app(config_name='DevelopmentConfig'):
    """Create Brazil Data Cube application from config object.

    Args:
        config_name (string) Config instance name
    Returns:
        Flask Application with config instance scope
    """
    from bdc_collectors.ext import CollectorExtension

    app = Flask(__name__)
    conf = config.get_settings(config_name)
    app.config.from_object(conf)

    with app.app_context():
        # Setup Celery Models
        load_celery_models()

        # Initialize Flask SQLAlchemy
        BDCCatalog(app)

        # Initialize Collector Extension
        CollectorExtension(app)

        # Just make sure to initialize db before celery
        celery_app = celery.create_celery_app(app)
        celery.celery_app = celery_app

        # Setup blueprint
        from .views import bp
        app.register_blueprint(bp)

        @app.after_request
        def after_request(response):
            """Enable CORS."""
            response.headers.add('Access-Control-Allow-Origin', '*')
            response.headers.add('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept')
            return response

        @app.before_first_request
        def _init():
            app.redis = redis.from_url(app.config['REDIS_URL'])

        class ImprovedJSONEncoder(JSONEncoder):
            def default(self, o):
                from datetime import datetime

                if isinstance(o, set):
                    return list(o)
                if isinstance(o, datetime):
                    return o.isoformat()
                return super(ImprovedJSONEncoder, self).default(o)

        app.config['RESTPLUS_JSON'] = {'cls': ImprovedJSONEncoder}
        app.config['JSON_SORT_KEYS'] = False

    app.json_encoder = ImprovedJSONEncoder

    setup_error_handlers(app)

    return app


def setup_error_handlers(app: Flask):
    """Configure Cube Builder Error Handlers on Flask Application."""
    @app.errorhandler(Exception)
    def handle_exception(e):
        """Handle exceptions."""
        if isinstance(e, HTTPException):
            return {'code': e.code, 'description': e.description}, e.code

        app.logger.exception(e)

        return {'code': InternalServerError.code,
                'description': InternalServerError.description}, InternalServerError.code


__all__ = (
    '__version__',
    'create_app',
)
