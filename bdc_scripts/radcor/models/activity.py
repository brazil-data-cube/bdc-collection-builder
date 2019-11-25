from sqlalchemy import Column, DateTime, Integer, String, Time
from sqlalchemy.orm import relationship

from bdc_scripts.models.base_sql import BaseModel


class RadcorActivity(BaseModel):
    __tablename__ = 'activities'
    __table_args__ = dict(schema='radcor')

    id = Column(Integer, primary_key=True)
    app = Column('app', String(64), nullable=False)
    sceneid = Column('sceneid', String(64), nullable=False)
    satellite = Column('satellite', String(8))
    priority = Column('priority', Integer)
    status = Column('status', String(16))
    link = Column('link', String(256))
    file = Column('file', String(128))
    start = Column('start', DateTime)
    end = Column('end', DateTime)
    elapsed = Column('elapsed', Time)
    retcode = Column('retcode', Integer)
    message = Column('message', String(512))

    history = relationship('RadcorActivityHistory', back_populates='activity')

    @classmethod
    def get_historic_by_task(cls, task_id: str):
        return cls.query().filter(cls.history.has(task_id=task_id)).all()

    @classmethod
    def is_started_or_done(cls, sceneid: str):
        return cls.query().filter(cls.sceneid == sceneid).all()