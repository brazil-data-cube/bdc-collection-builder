"""
Defines a structure component to run celery worker

Usage:

$ celery -A bdc_scripts.celery.worker:celery -l INFO -Q download
"""

# Python Native
import logging

# 3rdparty
from celery.signals import task_received, worker_shutdown
from celery.backends.database import Task
from celery.states import PENDING

# BDC Scripts
from bdc_scripts import create_app
from bdc_scripts.celery import create_celery_app
from bdc_scripts.celery.utils import TaskActivityFactory
from bdc_scripts.models import db


app = create_app()
celery = create_celery_app(app)


@task_received.connect
def on_received_store_in_db(sender, request, **kwargs):
    """
    Signal handler of Celery Task Receiver

    Whenever task received, we must persist the task in database
    as 'PENDING' in order to keep execution history

    Args:
        sender Celery Task Sender
        request Celery Task context
        kwargs Extra parameters used to dispatch task
    """

    with app.app_context():
        t = Task(request.task_id)
        t.status = PENDING
        db.session.add(t)
        db.session.commit()

        if request._payload:
            arguments = request._payload[0]

            if len(arguments) > 0:
                context_name = arguments[0].get('app')

                handler = TaskActivityFactory.get(context_name)

                if handler:
                    handler(t, *arguments)
                else:
                    logging.debug('No handler to attach task')
            else:
                logging.debug('No arguments passed. Skipping task association')

        logging.debug('Setting task {} to PENDING on database'.format(request.task_id))


@worker_shutdown.connect
def on_shutdown_release_locks(sender, **kwargs):
    """
    Signal handler of Celery Worker shutdown

    Tries to release Redis Lock if there is.
    """

    from bdc_scripts.celery.cache import lock_handler

    logging.info('Turning off Celery...')
    lock_handler.release_all()