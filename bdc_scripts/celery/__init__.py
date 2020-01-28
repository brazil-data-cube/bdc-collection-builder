from celery import Celery
from flask import Flask
from bdc_scripts.config import Config
from bdc_db.models import db
import logging
import flask


CELERY_TASKS = [
    'bdc_scripts.radcor.sentinel',
    'bdc_scripts.radcor.landsat'
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
        CELERY_RESULT_BACKEND='db+{}'.format(flask_app.config.get('SQLALCHEMY_DATABASE_URI')),
        # CELERY_TRACK_STARTED=True
    ))

    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            if not celery.conf.CELERY_ALWAYS_EAGER:
                if flask._app_ctx_stack.top is not None:
                    return TaskBase.__call__(self, *args, **kwargs)

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
                else:
                    try:
                        db.session.rollback()
                    except BaseException:
                        logging.warning('Error rollback transaction')
                        pass

            if not celery.conf.CELERY_ALWAYS_EAGER:
                db.session.remove()


    celery.Task = ContextTask

    return celery
