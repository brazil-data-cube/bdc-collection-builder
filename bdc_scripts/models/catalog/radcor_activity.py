from sqlalchemy import BigInteger, Column, DateTime, Index, Integer, String, Time
from bdc_scripts.models.base_sql import BaseModel


class RadcorActivity(BaseModel):
    __tablename__ = 'activities'
    __table_args__ = dict(schema='radcor')

    id = Column('id', BigInteger, nullable=False, unique=True, primary_key=True)
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
