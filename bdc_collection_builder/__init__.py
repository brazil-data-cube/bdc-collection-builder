#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Python Brazil Data Cube Collection Builder."""

from bdc_catalog.ext import BDCCatalog
from flask import Flask

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
    app = Flask(__name__)
    conf = config.get_settings(config_name)
    app.config.from_object(conf)

    with app.app_context():
        # Initialize Flask SQLAlchemy
        BDCCatalog(app)

        from bdc_collection_builder.db import db_aws
        db_aws.initialize()

        # Just make sure to initialize db before celery
        celery_app = celery.create_celery_app(app)
        celery.celery_app = celery_app

        # Setup blueprint
        from bdc_collection_builder.blueprint import bp
        app.register_blueprint(bp)

        load_celery_models()

        from .utils import initialize_factories

        @app.before_first_request
        def register_factories_on_init(*args):
            """Load Brazil Data Cube factories on init."""
            initialize_factories()

        @app.after_request
        def after_request(response):
            """Enable CORS."""
            response.headers.add('Access-Control-Allow-Origin', '*')
            response.headers.add('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept')
            return response

    return app


__all__ = (
    '__version__',
    'create_app',
)
