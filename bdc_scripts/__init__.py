from bdc_db.ext import BDCDatabase
from flask import Flask
from bdc_scripts import config, celery
from bdc_scripts.config import get_settings


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
        # Initialize Flask SQLAlchemy
        BDCDatabase(app)

        from bdc_scripts.db import db_aws
        db_aws.initialize()

        # Just make sure to initialize db before celery
        celery_app = celery.create_celery_app(app)
        celery.celery_app = celery_app

        # Setup blueprint
        from bdc_scripts.blueprint import bp
        app.register_blueprint(bp)

    return app
