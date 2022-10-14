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
from bdc_catalog.models import Collection
from flask.cli import FlaskGroup

from . import create_app
from .collections.collect import create_provider, get_provider_order
from .collections.models import CollectionProviderSetting
from .collections.utils import get_provider, get_or_create_model


# Create bdc-collection-builder cli from bdc-db
@click.group(cls=FlaskGroup, create_app=create_app)
def cli():
    """Command line for Collection Builder."""


@cli.command('create-provider')
@click.option('-n', '--name', help='Data Provider Name', required=True)
@click.option('-d', '--description', help='Detailed description to fully explain the provider ', required=False)
@click.option('-u', '--url', help='URL to locate extra info for provider.')
@click.option('--driver-name', help='Name of driver used to collect. Check BDC-Collectors', required=True)
@click.option('--username', help='Provider user')
@click.option('--password', help='Provider passwd')
@click.option('--credentials', help='JSON credentials', required=False)
def _create_provider(name: str, driver_name: str, description=None, url=None, credentials=None,
                     username=None, password=None):
    """Create definition for Provider and data collector."""
    if username is None and password is None and credentials is None:
        raise click.MissingParameter('No credential set. Use username/password or credentials.')

    if username or password:
        credentials = dict(username=username, password=password)

    provider, created = create_provider(name, driver_name=driver_name,
                                        description=description, url=url,
                                        **credentials)
    msg = 'created' if created else 'skipped.'
    click.secho(f'Collection {provider.name} {msg}', fg='green', bold=True)


@cli.command()
@click.option('-i', '--ifile', type=click.Path(exists=True, file_okay=True, readable=True))
@click.option('--from-dir', type=click.Path(exists=True, dir_okay=True, readable=True))
@click.option('-v', '--verbose', is_flag=True, default=False)
def load_providers(ifile: str, from_dir: str, verbose: bool):
    """Command line to load providers JSON into database.

    Note:
        Make sure you have exported variable ``SQLALCHEMY_DATABASE_URI`` before
        like ``SQLALCHEMY_DATABASE_URI=postgresql://postgres:postgres@localhost/bdc``.

    Note:
        It skips provider that already exists.
        You must give at least ``--ifile`` or ``--from_dir`` parameter.

    To load a single JSON file, use parameter ``-i`` or verbose ``--ifile path/to/json``::

        bdc-collection-builder load-provider --ifile examples/data/providers/nasa-usgs.json -v

    The following output will be displayed::

        Provider USGS created

    If you would like to read a directory containing several JSON collection files::

        bdc-collection-builder load-provider --from-dir examples/data/providers

    Args:
        ifile (str): Path to JSON file. Default is ``None``.
        from_dir (str): Readable directory containing JSON files. Defaults to ``None``.
        verbose (bool): Display verbose output. Defaults to ``False``.
    """
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
        credentials = data.pop('credentials', {})
        provider_setting, created = create_provider(**data, **credentials)
        msg = 'created' if created else 'skipped.'
        click.secho(f'Provider {provider_setting.name} {msg}', fg='green', bold=True)


@cli.command()
@click.option('-c', '--collection', help='The collection name and version as identifier.',
              type=click.STRING, required=True)
@click.option('--provider', type=click.STRING, required=True)
@click.option('--priority', type=click.IntRange(min=0), default=1, help='Priority order. High priority near 0')
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
    instance, _ = get_or_create_model(CollectionProviderSetting, defaults=data, **data)
    instance.active = not disable
    instance.priority = priority
    instance.save()
    click.secho(f'Collection Provider {collection.name}-{collection.version} updated', fg='green', bold=True)


@cli.command()
@click.option('-c', '--collection', help='The collection name and version as identifier.',
              type=click.STRING, required=True)
def overview(collection: str):
    """Describe information for Collection, which includes the data collect order by default."""
    collection = Collection.get_by_id(collection_id=collection)

    order = get_provider_order(collection)

    click.secho(f'Collection {collection.identifier}')
    for prop in ['title', 'name', 'version', 'description', 'collection_type']:
        value = getattr(collection, prop)
        if isinstance(value, str) and len(value) > 100:
            value = value[:100] + '...'
        click.secho(f'-> {click.style(prop, bold=True)}: {value}')
    click.secho('-> Providers:', bold=True)
    for entry in order:
        click.secho(f'  - {entry.instance.name}, driver={entry.provider_name}, '
                    f'priority={entry.priority}, active={entry.active}')


def main(as_module=False):
    """Load Brazil Data Cube (bdc_collection_builder) as module."""
    import sys
    cli.main(args=sys.argv[1:], prog_name="python -m bdc_collection_builder" if as_module else None)
