#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Defines a structure component to run celery worker."""

# Python Native
import logging
# 3rdparty
from celery.signals import celeryd_after_setup, worker_shutdown
# Builder
from .. import create_app
from ..utils import initialize_factories, finalize_factories
from . import create_celery_app


app = create_app()
celery = create_celery_app(app)


@celeryd_after_setup.connect
def register_factories_on_init(*args, **kwargs):
    """Register the Brazil Data Cube factories when celery is ready."""
    initialize_factories()

    logging.info('Factories loaded.')


@worker_shutdown.connect
def on_shutdown_release_locks(sender, **kwargs):
    """Signal handler of Celery Worker shutdown.

    Tries to release Redis Lock if there is.
    """
    finalize_factories()

    logging.info('Factories finalized.')
