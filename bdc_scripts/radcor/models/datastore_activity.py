from sqlalchemy import BigInteger, Column, Date, DateTime, Index, Integer, String, Time
from bdc_scripts.models.base_sql import BaseModel


class DataStoreActivity(BaseModel):
    __tablename__ = 'activities'
    __table_args__ = (
        Index('sceneid', 'tsceneid', 'band'),
        dict(schema='datastore')
    )

    id = Column(BigInteger, primary_key=True)
    app = Column('app', String(64), nullable=False)
    datacube = Column('datacube', String(32))
    tileid = Column('tileid', String(16))
    start = Column('start', Date, nullable=False)
    end = Column('end', Date, nullable=False)
    ttable = Column('ttable', String(16))
    tid = Column('tid', BigInteger)
    tsceneid = Column('tsceneid', String(64))
    band = Column('band', String(16))
    priority = Column('priority', Integer)
    status = Column('status', String(16))
    pstart = Column('pstart', DateTime)
    pend = Column('pend', DateTime)
    elapsed = Column('elapsed', Time)
    retcode = Column('retcode', Integer)
    message = Column('message', String(512))
