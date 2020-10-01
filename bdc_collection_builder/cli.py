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
from bdc_catalog.cli import cli
from flask.cli import with_appcontext, FlaskGroup

from . import create_app


# Create bdc-collection-builder cli from bdc-db
@click.group(cls=FlaskGroup, create_app=create_app)
def cli():
    """Command line for Collection Builder."""


@cli.group()
@with_appcontext
def scenes():
    """Handle collection images"""


@scenes.command()
@click.option('-c', '--collection', required=True, help='Collection name')
@click.option('--scene-ids', required=True, help='Given scene id to download')
@with_appcontext
def download(collection, scene_ids):
    """Download the Landsat-8 products using scene id.

    TODO: Support Sentinel 2 and Landsat 5/7.
    """
    from bdc_catalog.models import Collection
    from .collections.business import RadcorBusiness
    from .collections.utils import get_earth_explorer_api, EARTH_EXPLORER_DOWNLOAD_URI, EARTH_EXPLORER_PRODUCT_ID
    from .utils import initialize_factories

    initialize_factories()

    scenes = scene_ids.split(',')

    api = get_earth_explorer_api()

    dataset = 'LANDSAT_8_C1'

    collection = Collection.query().filter(Collection.name == collection).first_or_404()

    for scene in scenes:
        formal = api.lookup(dataset, [scene], inverse=True)

        link = EARTH_EXPLORER_DOWNLOAD_URI.format(folder=EARTH_EXPLORER_PRODUCT_ID[dataset], sid=formal[0])

        activity = dict(
            collection_id=collection.id,
            activity_type='downloadLC8',
            tags=[],
            sceneid=scene,
            scene_type='SCENE',
            args=dict(link=link)
        )

        _ = RadcorBusiness.create_activity(activity)

        RadcorBusiness.start(activity)


def main(as_module=False):
    """Load Brazil Data Cube (bdc_collection_builder) as module."""
    import sys
    cli.main(args=sys.argv[1:], prog_name="python -m bdc_collection_builder" if as_module else None)
