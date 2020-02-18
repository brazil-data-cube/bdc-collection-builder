#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Describe Task History Execution."""

from celery.backends.database import Task
from sqlalchemy import Column, DateTime, JSON, Integer, String, Time, or_, ForeignKey
from sqlalchemy.orm import relationship
from bdc_db.models.base_sql import db, BaseModel

from ...config import Config

class RadcorActivityHistory(BaseModel):
    """Define Activity History execution.

    This model is attached with celery execution.
    An activity may have multiple executions 1..N.
    """

    __tablename__ = 'activity_history'
    __table_args__ = dict(schema=Config.ACTIVITIES_SCHEMA)

    activity_id = Column(ForeignKey('{}.activities.id'.format(Config.ACTIVITIES_SCHEMA)), primary_key=True, nullable=False)
    task_id = Column(ForeignKey(Task.id), primary_key=True, nullable=False)

    start = Column('start', DateTime)
    env = Column('env', JSON)

    # Relations
    activity = relationship('RadcorActivity', back_populates="history")
    task = relationship(Task, uselist=False)

    @classmethod
    def get_by_task_id(cls, task_id: str):
        """Retrieve a task execution from celery task id."""
        return cls.query().filter(cls.task.has(task_id=task_id)).one()
