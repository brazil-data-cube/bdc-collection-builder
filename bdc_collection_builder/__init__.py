from bdc_db.ext import BDCDatabase
from flask import Flask
from flask_cors import CORS
from bdc_collection_builder import config, celery
from bdc_collection_builder.config import get_settings


def create_app(config_name='DevelopmentConfig'):
    """
    Creates Brazil Data Cube application from config object
    Args:
        config_name (string) Config instance name
    Returns:
        Flask Application with config instance scope
    """

    app = Flask(__name__)
    conf = config.get_settings(config_name)
    app.config.from_object(conf)

    with app.app_context():
        cors = CORS(app, resources={r"/*": {"origins": "*"}})

        # Initialize Flask SQLAlchemy
        BDCDatabase(app)

        from bdc_collection_builder.db import db_aws
        db_aws.initialize()

        # Just make sure to initialize db before celery
        celery_app = celery.create_celery_app(app)
        celery.celery_app = celery_app

        # Setup blueprint
        from bdc_collection_builder.blueprint import bp
        app.register_blueprint(bp)

    return app
