from sqlalchemy import ARRAY, Column, DateTime, ForeignKey, Integer, JSON, String, Time
from sqlalchemy.orm import relationship

from bdc_db.models.base_sql import BaseModel


class RadcorActivity(BaseModel):
    __tablename__ = 'activities'

    id = Column(Integer, primary_key=True)
    collection_id = Column(ForeignKey('collections.id'), nullable=False)
    activity_type = Column('activity_type', String(64), nullable=False)
    args = Column('args', JSON)
    tags = Column('tags', ARRAY(String))
    scene_type = Column('scene_type', String)
    sceneid = Column('sceneid', String(64), nullable=False)

    # Relations
    collection = relationship('Collection')
    history = relationship('RadcorActivityHistory', back_populates='activity')

    @classmethod
    def get_historic_by_task(cls, task_id: str):
        return cls.query().filter(cls.history.has(task_id=task_id)).all()

    @classmethod
    def is_started_or_done(cls, sceneid: str):
        return cls.query().filter(cls.sceneid == sceneid).all()