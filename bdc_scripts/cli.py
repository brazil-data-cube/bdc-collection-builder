"""
Brazil Data Cube Scripts

Creates a python click context and inject it to the global flask commands

It allows to call own
"""


import click
from flask.cli import FlaskGroup, with_appcontext
from flask_migrate.cli import db as flask_migrate_db
from sqlalchemy_utils.functions import create_database, database_exists
from bdc_db.models import db
from bdc_scripts import create_app


def create_cli(create_app=None):
    """
    Wrapper creation of Flask App in order to attach into flask click

    Args:
         create_app (function) - Create app factory (Flask)
    """
    def create_cli_app(info):
        if create_app is None:
            info.create_app = None

            app = info.load_app()
        else:
            app = create_app()

        return app

    @click.group(cls=FlaskGroup, create_app=create_cli_app)
    def cli(**params):
        """Command line interface for bdc-scripts"""
        pass

    return cli


cli = create_cli(create_app=create_app)


@flask_migrate_db.command()
@with_appcontext
def create():
    """Create database. Make sure the variable SQLALCHEMY_DATABASE_URI is set"""
    click.secho('Creating database {0}'.format(db.engine.url),
                fg='green')
    if not database_exists(str(db.engine.url)):
        create_database(str(db.engine.url))

    click.secho('Creating schemas...', fg='green')
    with db.session.begin_nested():
        db.session.execute('CREATE SCHEMA IF NOT EXISTS catalogo')
        db.session.execute('CREATE SCHEMA IF NOT EXISTS datastorm')
        db.session.execute('CREATE SCHEMA IF NOT EXISTS radcor')

    click.secho('Creating extension postgis...', fg='green')
    with db.session.begin_nested():
        db.session.execute('CREATE EXTENSION IF NOT EXISTS postgis')

    db.session.commit()

