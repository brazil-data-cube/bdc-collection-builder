from sqlalchemy import Column, Date, DateTime, String, Integer, BigInteger, Float, text, Index
from bdc_scripts.models.base_sql import BaseModel


class Qlook(BaseModel):
    __tablename__ = 'qlook'
    __table_args__ = dict(schema='datastore')

    id = Column(BigInteger, primary_key=True)
    datacube = Column(String(48), nullable=False)
    tileid = Column(String(16), nullable=False)
    start = Column(Date, nullable=False)
    end = Column(Date, nullable=False)
    sceneid = Column(String(64), nullable=False, index=True)
    qlookfile = Column(String(256), nullable=False)