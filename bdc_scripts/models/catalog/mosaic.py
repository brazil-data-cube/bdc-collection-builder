from sqlalchemy import Column, Date, String, Integer, BigInteger, Float, text, Index
from bdc_scripts.models.base_sql import BaseModel


class Mosaic(BaseModel):
    __tablename__ = 'mosaics'
    __table_args__ = (
        Index('general', 'datacube', 'tileid', 'start', 'end', unique=True),
        dict(schema='datastore')
    )

    id = Column(BigInteger, primary_key=True)
    datacube = Column(String(32), nullable=False)
    tileid = Column(String(16), nullable=False)
    start = Column(Date, nullable=False)
    end = Column(Date, nullable=False)
    numcol = Column(Integer, nullable=False)
    numlin = Column(Integer, nullable=False)