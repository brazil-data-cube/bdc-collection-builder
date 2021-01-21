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
from flask.cli import FlaskGroup

from . import create_app


# Create bdc-collection-builder cli from bdc-db
@click.group(cls=FlaskGroup, create_app=create_app)
def cli():
    """Command line for Collection Builder."""


def main(as_module=False):
    """Load Brazil Data Cube (bdc_collection_builder) as module."""
    import sys
    cli.main(args=sys.argv[1:], prog_name="python -m bdc_collection_builder" if as_module else None)
