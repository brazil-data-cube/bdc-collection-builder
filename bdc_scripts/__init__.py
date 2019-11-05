import os
from flask import Flask
from flask_bcrypt import Bcrypt
from flask_cors import CORS
from flask_migrate import Migrate
from bdc_scripts import celery
from bdc_scripts.config import get_settings
from bdc_scripts.models import db


def create_app(config_name='DevelopmentConfig'):
    """
    Creates Brazil Data Cube application from config object
    Args:
        config_name (string) Config instance name
    Returns:
        Flask Application with config instance scope
    """

    app = Flask(__name__)

    with app.app_context():
        app.config.from_object(get_settings(config_name))

        # Initialize Flask SQLAlchemy
        db.init_app(app)

        Migrate(app, db)

        # Just make sure to initialize db before celery
        celery_app = celery.create_celery_app(app)
        celery.celery_app = celery_app

        # Setup blueprint
        from bdc_scripts.blueprint import bp
        app.register_blueprint(bp)

        flask_bcrypt = Bcrypt()
        flask_bcrypt.init_app(app)

        CORS(app, resorces={r'/d/*': {"origins": '*'}})

    return app
