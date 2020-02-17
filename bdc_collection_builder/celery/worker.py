#
# This file is part of BDC Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# BDC Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Defines a structure component to run celery worker."""

# Python Native
import logging
# 3rdparty
from celery.signals import worker_shutdown
# Builder
from bdc_collection_builder import create_app
from bdc_collection_builder.celery import create_celery_app


app = create_app()
celery = create_celery_app(app)


@worker_shutdown.connect
def on_shutdown_release_locks(sender, **kwargs):
    """Signal handler of Celery Worker shutdown.

    Tries to release Redis Lock if there is.
    """
    from bdc_collection_builder.celery.cache import lock_handler

    logging.info('Turning off Celery...')
    lock_handler.release_all()
