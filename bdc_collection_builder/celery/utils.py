#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Defines the utility functions to use among celery tasks."""

from celery import current_app
from celery.backends.database import SessionManager


def list_running_tasks():
    """List all running tasks in celery cluster."""
    inspector = current_app.control.inspect()

    return inspector.active()


def list_pending_tasks():
    """List all pending tasks in celery cluster."""
    inspector = current_app.control.inspect()

    return inspector.reserved()


def load_celery_models():
    """Prepare and load celery models in database backend."""
    session = SessionManager()
    engine = session.get_engine(current_app.backend.url)
    session.prepare_models(engine)
