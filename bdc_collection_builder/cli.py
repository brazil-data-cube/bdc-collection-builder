#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Define Brazil Data Cube command line utilities.

Creates a python click context and inject it to the global flask commands.
"""

import click
from bdc_db.models import db
from bdc_db.cli import create_db as bdc_create_db, create_cli
from flask.cli import FlaskGroup, with_appcontext
from flask_migrate.cli import db as flask_db

from . import create_app
from .config import Config


# Create bdc-collection-builder cli from bdc-db
cli = create_cli(create_app=create_app)


@flask_db.command()
@with_appcontext
@click.pass_context
def create_db(ctx: click.Context):
    """Create database. Make sure the variable SQLALCHEMY_DATABASE_URI is set."""

    # Forward context to bdc-db createdb command in order to create database
    ctx.forward(bdc_create_db)

    click.secho('Creating schema {}...'.format(Config.ACTIVITIES_SCHEMA), fg='green')
    with db.session.begin_nested():
        db.session.execute('CREATE SCHEMA IF NOT EXISTS {}'.format(Config.ACTIVITIES_SCHEMA))

    db.session.commit()


def main(as_module=False):
    """Load Brazil Data Cube (bdc_collection_builder) as module."""
    import sys
    cli.main(args=sys.argv[1:], prog_name="python -m bdc_collection_builder" if as_module else None)
