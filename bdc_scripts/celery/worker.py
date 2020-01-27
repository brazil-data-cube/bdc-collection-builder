"""
Defines a structure component to run celery worker

Usage:

$ celery -A bdc_scripts.celery.worker:celery -l INFO -Q download
"""

# Python Native
import logging

# 3rdparty
from celery.signals import worker_shutdown

# BDC Scripts
from bdc_scripts import create_app
from bdc_scripts.celery import create_celery_app


app = create_app()
celery = create_celery_app(app)


@worker_shutdown.connect
def on_shutdown_release_locks(sender, **kwargs):
    """
    Signal handler of Celery Worker shutdown

    Tries to release Redis Lock if there is.
    """

    from bdc_scripts.celery.cache import lock_handler

    logging.info('Turning off Celery...')
    lock_handler.release_all()
