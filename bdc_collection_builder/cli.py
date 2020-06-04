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
from bdc_db.cli import create_cli
from bdc_db.cli import create_db as bdc_create_db
from bdc_db.models import Band, Collection, db
from bdc_db.cli import create_db as bdc_create_db, create_cli
from flask.cli import with_appcontext
from flask_migrate.cli import db as flask_db

from . import create_app
from .collections.sentinel.utils import Sentinel2SR
from .collections.utils import get_or_create_model
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


@cli.command('load-collections')
@with_appcontext
def load_collections():
    """Load initial collections for Sentinel 2 on Collection Builder."""
    with db.session.begin_nested():
        defaults = dict(
            id='S2_MSI_L2_SR_LASRC',
            grs_schema_id='MGRS',
            description='Sentinel 2A/2B Surface Reflectance using laSRC 2.0 and Fmask 4',
            geometry_processing='ortho',
            is_cube=False,
            radiometric_processing='SR',
            sensor='MSI',
            bands_quicklook='red,green,blue',
            composite_function_schema_id='IDENTITY',
        )

        collection, _ = get_or_create_model(Collection, defaults=defaults, id=defaults['id'])

        bands = Sentinel2SR.get_band_map(None)

        for band_name, common_name in bands.items():
            where = dict(
                name=band_name, common_name=common_name, collection_id=collection.id
            )

            resolution = 10
            data_type = 'int16'
            min_value, max_value = 1, 10000
            fill = 0
            scale = '0.0001'

            if common_name == 'quality':
                data_type = 'Byte'
                max_value = 12
                scale = '1'

            band_defaults = dict(
                name=band_name,
                common_name=common_name,
                collection_id=collection.id,
                min=min_value,
                max=max_value,
                fill=fill,
                scale=scale,
                data_type=data_type,
                mime_type='image/tiff',
                resolution_unit='m',
                resolution_x=resolution,
                resolution_y=resolution,
            )

            band, _ = get_or_create_model(Band, defaults=band_defaults, **where)

    db.session.commit()


def main(as_module=False):
    """Load Brazil Data Cube (bdc_collection_builder) as module."""
    import sys
    cli.main(args=sys.argv[1:], prog_name="python -m bdc_collection_builder" if as_module else None)
