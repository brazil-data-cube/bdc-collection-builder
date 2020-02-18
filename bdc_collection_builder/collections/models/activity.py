#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Describe a collection activity."""

from sqlalchemy import ARRAY, Column, ForeignKey, Integer, JSON, String
from sqlalchemy.orm import relationship
from bdc_db.models import Collection
from bdc_db.models.base_sql import BaseModel

from ...config import Config


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
