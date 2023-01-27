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

"""Models for Collection Builder."""
from typing import Optional

from bdc_catalog.models import Collection, Provider
from bdc_catalog.models.base_sql import BaseModel, db
from celery.backends.database import Task
from celery import states
from sqlalchemy import (ARRAY, JSON, Column, DateTime, ForeignKey, Integer,
                        Index, PrimaryKeyConstraint, String, UniqueConstraint)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from ..config import Config

db.metadata.schema = Config.ACTIVITIES_SCHEMA


class RadcorActivity(BaseModel):
    """Define a collection activity.

    An activity consists in task to execute.
    """

    __tablename__ = 'activities'

    id = Column(Integer, primary_key=True)
    collection_id = Column(ForeignKey(Collection.id), nullable=False)
    activity_type = Column('activity_type', String(64), nullable=False)
    args = Column('args', JSON)
    tags = Column('tags', ARRAY(String))
    scene_type = Column('scene_type', String)
    sceneid = Column('sceneid', String(255), nullable=False)

    # Relations
    collection = relationship('Collection')
    history = relationship('RadcorActivityHistory', back_populates='activity', order_by='desc(RadcorActivityHistory.start)')

    children = relationship('ActivitySRC', primaryjoin='RadcorActivity.id == ActivitySRC.activity_src_id')
    parents = relationship('ActivitySRC', primaryjoin='RadcorActivity.id == ActivitySRC.activity_id')

    __table_args__ = (
        UniqueConstraint(collection_id, activity_type, sceneid),
        dict(schema=Config.ACTIVITIES_SCHEMA),
    )


class ActivitySRC(BaseModel):
    """Model for collection provenance/lineage."""

    __tablename__ = 'activity_src'

    activity_id = db.Column(
        db.Integer(),
        db.ForeignKey(RadcorActivity.id, onupdate='CASCADE', ondelete='CASCADE'),
        nullable=False)

    activity_src_id = db.Column(
        db.Integer(),
        db.ForeignKey(RadcorActivity.id, onupdate='CASCADE', ondelete='CASCADE'),
        nullable=False)

    activity = relationship(RadcorActivity, primaryjoin='ActivitySRC.activity_id == RadcorActivity.id')
    parent = relationship(RadcorActivity, primaryjoin='ActivitySRC.activity_src_id == RadcorActivity.id')

    __table_args__ = (
        PrimaryKeyConstraint(activity_id, activity_src_id),
        dict(schema=Config.ACTIVITIES_SCHEMA),
    )


class RadcorActivityHistory(BaseModel):
    """Define Activity History execution.

    This model is attached with celery execution.
    An activity may have multiple executions 1..N.
    """

    __tablename__ = 'activity_history'
    __table_args__ = dict(schema=Config.ACTIVITIES_SCHEMA)

    activity_id = Column(
        ForeignKey('{}.activities.id'.format(Config.ACTIVITIES_SCHEMA, onupdate='CASCADE', ondelete='CASCADE')),
        primary_key=True, nullable=False
    )
    task_id = Column(ForeignKey(Task.id, onupdate='CASCADE', ondelete='CASCADE'), primary_key=True, nullable=False)

    start = Column('start', DateTime)
    env = Column('env', JSON)

    # Relations
    activity = relationship('RadcorActivity', back_populates="history")
    task = relationship(Task, uselist=False)

    @classmethod
    def get_by_task_id(cls, task_id: str):
        """Retrieve a task execution from celery task id."""
        return cls.query().filter(cls.task.has(task_id=task_id)).one()

    @hybrid_property
    def status(self):
        return self.task.status

    @hybrid_property
    def is_running(self):
        return self.status == states.STARTED

    @hybrid_property
    def is_done(self):
        return self.status in (states.SUCCESS, states.FAILURE)


class ProviderSetting(BaseModel):
    """Model for table ``collection_builder.provider_settings``.

    This model bridges a relationship with a BDC Catalog Provider with
    the internal data collector driver.
    For example, the ESA Copernicus Provider is attached with the driver
    `SciHub <https://bdc-collectors.readthedocs.io/en/latest/usage.html#scihub>`_
    in the package `bdc-collectors`.
    """

    __tablename__ = 'provider_settings'

    id = Column(db.Integer, primary_key=True, autoincrement=True)
    provider_id = Column(
        ForeignKey(Provider.id, onupdate='CASCADE', ondelete='CASCADE'),
        nullable=False
    )
    driver_name = Column(db.String(64))
    """The driver name supported by bdc-collectors."""
    credentials = Column(JSONB)
    """The driver catalog access credentials."""

    provider = relationship(Provider)

    __table_args__ = (
        Index(None, provider_id),
        UniqueConstraint(provider_id, driver_name),  # Restrict only Provider Name with Driver
        dict(schema=Config.ACTIVITIES_SCHEMA),
    )

    @property
    def name(self):
        return self.provider.name


class CollectionProviderSetting(BaseModel):
    """Track the available data providers for an image collection."""

    __tablename__ = 'collections_providers_settings'

    provider_id = Column('provider_id', db.Integer(),
                         ForeignKey(ProviderSetting.id, onupdate='CASCADE', ondelete='CASCADE'),
                         nullable=False, primary_key=True)

    collection_id = Column('collection_id', db.Integer(),
                           ForeignKey(Collection.id, onupdate='CASCADE', ondelete='CASCADE'),
                           nullable=False, primary_key=True)

    active = Column(db.Boolean(), nullable=False, default=True)
    priority = Column(db.SmallInteger(), nullable=False)

    __table_args__ = (
        Index(None, active),
        dict(schema=Config.ACTIVITIES_SCHEMA),
    )

    provider_setting = relationship(ProviderSetting)
    collection = relationship(Collection)

    @property
    def provider(self) -> Optional[Provider]:
        """The BDC Catalog provider instance."""
        return self.provider_setting.provider
