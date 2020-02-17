#
# This file is part of BDC Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# BDC Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Define Brazil Data Cube command line utilities.

Creates a python click context and inject it to the global flask commands.
"""

import click
from bdc_db.models import db
from flask.cli import FlaskGroup, with_appcontext
from flask_migrate.cli import db as flask_migrate_db
from sqlalchemy_utils.functions import create_database, database_exists

from . import create_app


def create_cli(create_app=None):
    """Define a Wrapper creation of Flask App in order to attach into flask click.

    Args:
         create_app (function) - Create app factory (Flask)
    """
    def create_cli_app(info):
        """Describe flask factory to create click command."""
        if create_app is None:
            info.create_app = None

            app = info.load_app()
        else:
            app = create_app()

        return app

    @click.group(cls=FlaskGroup, create_app=create_cli_app)
    def cli(**params):
        """Command line interface for bdc_collection_builder."""
        pass

    return cli


cli = create_cli(create_app=create_app)


@flask_migrate_db.command()
@with_appcontext
def create():
    """Create database. Make sure the variable SQLALCHEMY_DATABASE_URI is set."""
    click.secho('Creating database {0}'.format(db.engine.url),
                fg='green')
    if not database_exists(str(db.engine.url)):
        create_database(str(db.engine.url))

    click.secho('Creating extension postgis...', fg='green')
    with db.session.begin_nested():
        db.session.execute('CREATE EXTENSION IF NOT EXISTS postgis')

    db.session.commit()


def main(as_module=False):
    """Load Brazil Data Cube (bdc_collection_builder) as module."""
    import sys
    cli.main(args=sys.argv[1:], prog_name="python -m bdc_collection_builder" if as_module else None)
