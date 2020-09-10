#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Models for Collection Builder."""

from bdc_catalog.models import Collection
from bdc_catalog.models.base_sql import BaseModel, db
from celery.backends.database import Task
from sqlalchemy import ARRAY, Column, DateTime, Integer, ForeignKey, JSON, String
from sqlalchemy.orm import relationship

from ..config import Config


db.metadata.schema = Config.ACTIVITIES_SCHEMA


class RadcorActivity(BaseModel):
    """Define a collection activity.

    An activity consists in task to execute.
    """

    __tablename__ = 'activities'
    __table_args__ = dict(schema=Config.ACTIVITIES_SCHEMA)

    id = Column(Integer, primary_key=True)
    collection_id = Column(ForeignKey(Collection.id), nullable=False)
    activity_type = Column('activity_type', String(64), nullable=False)
    args = Column('args', JSON)
    tags = Column('tags', ARRAY(String))
    scene_type = Column('scene_type', String)
    sceneid = Column('sceneid', String(64), nullable=False)

    # Relations
    collection = relationship('Collection')
    history = relationship('RadcorActivityHistory', back_populates='activity', order_by='desc(RadcorActivityHistory.start)')


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
