from sqlalchemy import Column, Date, String, Integer, BigInteger, Float, text
from bdc_scripts.models.base_sql import BaseModel


class Datacube(BaseModel):
    __tablename__ = 'datacubes'
    __table_args__ = dict(schema='datastore')

    id = Column(BigInteger, primary_key=True)
    datacube = Column(String(48), nullable=False)
    wrs = Column(String(16), nullable=False)
    tschema = Column(String(16), nullable=False)
    step = Column(Integer, nullable=False)
    satsen = Column(String(32), nullable=False)
    bands = Column(String(128), nullable=False)
    quicklook = Column(String(64), nullable=False, server_default=text("'swir2,nir,red'"))
    start = Column(Date)
    end = Column(Date)
    resx = Column(Float, nullable=False)
    resy = Column(Float, nullable=False)