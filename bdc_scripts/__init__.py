import os
from flask import Flask
from flask_migrate import Migrate
from bdc_scripts.models import db


def create_app():
    internal_app = Flask(__name__)

    with internal_app.app_context():
        internal_app.config.setdefault('SQLALCHEMY_DATABASE_URI', os.environ.get('SQLALCHEMY_DATABASE_URI'))
        Migrate(internal_app, db)

    return internal_app


app = create_app()