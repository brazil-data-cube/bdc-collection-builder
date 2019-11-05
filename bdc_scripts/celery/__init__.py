import logging
from celery import Celery
from flask import Flask
from bdc_scripts.config import Config
from bdc_scripts.models import db


CELERY_TASKS = [
    'bdc_scripts.sentinel'
]

celery_app = None


def create_celery_app(flask_app: Flask):
    """
    Creates a Celery object and tir the celery config to the Flask app config

    Wrap all the celery tasks in the context of Flask application

    Args:
        flask_app (flask.Flask): Flask app

    Returns:
        Celery celery app
    """

    celery = Celery(
        flask_app.import_name,
        broker=Config.RABBIT_MQ_URL
    )

    # Load tasks
    celery.autodiscover_tasks(CELERY_TASKS)

    # Set same config of Flask into Celery flask_app
    celery.conf.update(flask_app.config)

    always_eager = flask_app.config.get('TESTING', False)
    celery.conf.update(dict(
        CELERY_TASK_ALWAYS_EAGER=always_eager,
        CELERY_RESULT_BACKEND='db+{}'.format(flask_app.config.get('SQLALCHEMY_DATABASE_URI'))
    ))

    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            if not celery.conf.CELERY_ALWAYS_EAGER:
                with flask_app.app_context():
                    # Following example of Flask
                    # Just make sure the task execution is running inside flask context
                    # https://flask.palletsprojects.com/en/1.1.x/patterns/celery/

                    return TaskBase.__call__(self, *args, **kwargs)
            else:
                logging.warning('Not Call context Task')

        def after_return(self, status, retval, task_id, args, kwargs, einfo):
            """
            Called after task execution.

            Whenever task finishes, it must teardown our db session, since the Flask SQLAlchemy
            creates scoped session at startup.
            FMI: https://gist.github.com/twolfson/a1b329e9353f9b575131
            """

            if flask_app.config['SQLALCHEMY_COMMIT_ON_TEARDOWN']:
                if not isinstance(retval, Exception):
                    db.session.commit()

            if not celery.conf.CELERY_ALWAYS_EAGER:
                db.session.remove()

    celery.Task = ContextTask

    return celery
