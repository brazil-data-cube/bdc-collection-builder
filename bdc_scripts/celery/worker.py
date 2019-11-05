"""
Defines a structure component to run celery worker

Usage:

$ celery -A bdc_scripts.celery.worker:celery -l INFO -Q download
"""

import logging
from bdc_scripts import create_app
from bdc_scripts.celery import create_celery_app
from bdc_scripts.models import db
from celery.signals import task_received, task_prerun, worker_shutdown
from celery.backends.database import Task
from celery.states import PENDING, STARTED


app = create_app()
celery = create_celery_app(app)


@task_received.connect
def on_received_store_in_db(sender=None, request=None, **kwargs):
    """
    Signal handler of Celery Task Receiver

    Whenever task received, we must persist the task in database in order
    to keep execution history

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

        logging.debug('Setting task {} to PENDING on database'.format(request.task_id))


@task_prerun.connect
def on_before_task_run(task_id, task, *args, **kwargs):
    """
    Signal handler of Celery Task Before Execution

    Whenever task is ready to execute, we must update the task state in database to
    STARTED.

    Args:
        task_id Task identifier
        task Task object context (task used to trigger)
        args Task Arguments
        kwargs Extra parameters used to dispatch task such sender and args
    """

    with app.app_context():
        t = db.session.query(Task).filter_by(task_id=task_id).one()

        t.status = STARTED
        db.session.commit()

        logging.debug('Setting task {} to STARTED on database'.format(task_id))


@worker_shutdown.connect
def on_shutdown_release_locks(sender, **kwargs):
    """
    Signal handler of Celery Worker shutdown

    Tries to release Redis Lock if there is.
    """

    from bdc_scripts.celery.cache import lock_handler

    logging.info('Turning off Celery...')
    lock_handler.release_all()