import os
from flask import Flask
from flask_bcrypt import Bcrypt
from flask_cors import CORS
from bdc_scripts.blueprint import bp
from bdc_scripts.config import get_settings


flask_bcrypt = Bcrypt()


def create_app(config_name):
    """
    Creates Brazil Data Cube WTSS application from config object
    Args:
        config_name (string|bdc_sample.config.Config) Config instance
    Returns:
        Flask Application with config instance scope
    """

    app = Flask(__name__)

    with app.app_context():
        app.config.from_object(config_name)
        app.register_blueprint(bp)

        flask_bcrypt.init_app(app)

    return app


app = create_app(get_settings(os.environ.get('ENVIRONMENT', 'DevelopmentConfig')))

CORS(app, resorces={r'/d/*': {"origins": '*'}})