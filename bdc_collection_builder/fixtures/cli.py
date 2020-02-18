#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Python Brazil Data Cube Collection Builder Fixture click client."""

from json import loads as json_parser

from bdc_db.models import db, Band, Collection, CompositeFunctionSchema, GrsSchema, Tile
from flask.cli import with_appcontext
from pkg_resources import resource_string
import click


@click.group(chain=True)
def fixtures():
    """Command for loading Brazil Data Cube Collection Builder data."""


@fixtures.command()
@with_appcontext
def init():
    """Initialize default fixtures."""
    load_fixtures()


def load_model(fixture_path: str, model_class):
    """Load fixture to database.

    Args:
        fixture_path - Path relative to fixtures. i.e 'data/tiles.json'
        model_class - SQLAlchemy Model Class
    """
    schemas = json_parser(resource_string('bdc_collection_builder.fixtures', fixture_path))

    with db.session.begin_nested():
        for schema in schemas:
            model = model_class(**schema)

            model.save(commit=False)


def load_collections(fixture_path: str):
    """Load default collections to database.

    Args:
        fixture_path - Path relative to fixtures. i.e 'data/tiles.json'
    """
    collections = json_parser(resource_string('bdc_collection_builder.fixtures', fixture_path))

    with db.session.begin_nested():
        for collection in collections:
            bands = collection.pop('bands')

            c = Collection(**collection)
            c.save(commit=False)

            for band in bands:
                b = Band(**band)
                b.collection = c

                b.save(commit=False)


def load_fixtures():
    """Load default database fixtures."""
    load_model('data/grs_schemas.json', GrsSchema)
    load_model('data/tiles.json', Tile)
    load_model('data/composite_functions.json', CompositeFunctionSchema)
    load_collections('data/collections.json')

    db.session.commit()
