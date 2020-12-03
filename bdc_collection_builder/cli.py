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
import warnings

import click
from bdc_catalog.cli import cli
from flask import current_app
from flask.cli import FlaskGroup, with_appcontext
from sqlalchemy.sql.ddl import CreateSchema

from . import create_app


# Create bdc-collection-builder cli from bdc-db
@click.group(cls=FlaskGroup, create_app=create_app)
def cli():
    """Command line for Collection Builder."""


@cli.command()
@click.pass_context
@with_appcontext
def create_namespaces(ctx):
    """Create all namespaces used in BDC-Collection-Builder and BDC-Catalog."""
    warnings.simplefilter('always', DeprecationWarning)
    warnings.warn(
        '\nThis command line utility is deprecated.'
        '\nUse latest `BDC-DB` and `BDC-Catalog` package with command line: '
        '\n\n\tbdc-db db create-namespaces'
        '\n\nSee more in https://bdc-db.readthedocs.io/en/latest/.',
        category=DeprecationWarning,
        stacklevel=1
    )

    from bdc_db.cli import create_namespace

    _db = current_app.extensions['bdc-catalog'].db

    if not _db.engine.dialect.has_schema(_db.engine, _db.metadata.schema):
        ctx.invoke(create_namespace)

    if not _db.engine.dialect.has_schema(_db.engine, 'bdc'):
        with _db.session.begin_nested():
            _db.session.execute(CreateSchema('bdc'))
        _db.session.commit()


def main(as_module=False):
    """Load Brazil Data Cube (bdc_collection_builder) as module."""
    import sys
    cli.main(args=sys.argv[1:], prog_name="python -m bdc_collection_builder" if as_module else None)
