#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2022 INPE.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/gpl-3.0.html>.
#

"""Define Brazil Data Cube command line utilities.

Creates a python click context and inject it to the global flask commands.
"""

import json
from pathlib import Path

import click
from bdc_catalog.cli import cli
from bdc_catalog.models import Collection, CollectionsProviders
from flask.cli import FlaskGroup

from . import create_app
from .collections.utils import create_collection, get_provider, get_or_create_model


# Create bdc-collection-builder cli from bdc-db
@click.group(cls=FlaskGroup, create_app=create_app)
def cli():
    """Command line for Collection Builder."""


@cli.command()
@click.option('-i', '--ifile', type=click.Path(exists=True, file_okay=True, readable=True))
@click.option('--from-dir', type=click.Path(exists=True, dir_okay=True, readable=True))
def load_data(ifile: str, from_dir: str):
    """Command line to load collections JSON into database."""
    entries = []
    if ifile:
        entries.append(Path(ifile))
    elif from_dir:
        for entry in Path(from_dir).glob('*.json'):
            entries.append(entry)
    else:
        raise click.MissingParameter("Missing --ifile or --from-dir parameter.")

    for entry in entries:
        with entry.open() as fd:
            data = json.load(fd)

        collection, created = create_collection(**data)
        msg = 'created' if created else 'skipped.'
        click.secho(f'Collection {collection.name}-{collection.version} {msg}', fg='green', bold=True)


@cli.command()
@click.option('-c', '--collection', help='The collection name and version as identifier.',
              type=click.STRING, required=True)
@click.option('--provider', type=click.STRING, required=True)
@click.option('--priority', type=click.IntRange(min=0), default=1)
@click.option('--disable', is_flag=True, default=False)
def set_provider(collection: str, provider: str, priority: int, disable: bool):
    fragments = collection.rsplit('-', 1)
    collection = (
        Collection.query()
        .filter(Collection.name == fragments[0],
                Collection.version == fragments[-1])
        .first()
    )
    provider, _ = get_provider(provider)
    data = {'collection_id': collection.id,
            'provider_id': provider.id}
    instance, _ = get_or_create_model(CollectionsProviders, defaults=data, **data)
    instance.active = not disable
    instance.priority = priority
    instance.save()
    click.secho(f'Collection Provider {collection.name}-{collection.version} updated', fg='green', bold=True)


def main(as_module=False):
    """Load Brazil Data Cube (bdc_collection_builder) as module."""
    import sys
    cli.main(args=sys.argv[1:], prog_name="python -m bdc_collection_builder" if as_module else None)
