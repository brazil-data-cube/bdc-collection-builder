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

"""Defines the utility functions to use among celery tasks."""

from celery import current_app
from celery.backends.database.models import Task, TaskSet


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
    from bdc_db.db import db

    Task.__table__.create(bind=db.engine, checkfirst=True)
    TaskSet.__table__.create(bind=db.engine, checkfirst=True)

