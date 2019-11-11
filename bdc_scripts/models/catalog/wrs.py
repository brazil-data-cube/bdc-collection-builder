from sqlalchemy import Column, Float, Index, Integer, String
from bdc_scripts.models.base_sql import BaseModel


class WRS(BaseModel):
    __tablename__ = 'wrs'
    __tableargs__ = (
        Index('npr', 'name', 'tileid', unique=True),
        Index('geo', 'lonmin', 'lonmax', 'latmin', 'latmax', unique=True),
        dict(schema='datastore')
    )

    id = Column(Integer, primary_key=True)
    name = Column('name', String(16), nullable=False)
    path = Column('path', Integer, nullable=False)
    row = Column('row', Integer, nullable=False)
    tileid = Column('tileid', String(16), nullable=False)
    xmin = Column('xmin', Float, nullable=False)
    xmax = Column('xmax', Float, nullable=False)
    ymin = Column('ymin', Float, nullable=False)
    ymax = Column('ymax', Float, nullable=False)
    lonmin = Column('lonmin', Float, nullable=False)
    lonmax = Column('lonmax', Float, nullable=False)
    latmin = Column('latmin', Float, nullable=False)
    latmax = Column('latmax', Float, nullable=False)
    srs = Column('srs', String(128), nullable=False)
    geom = Column('geom', String(1024), nullable=False)