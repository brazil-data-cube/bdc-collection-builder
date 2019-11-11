from sqlalchemy import Column, Date, DateTime, String, Integer, BigInteger, Float, text, Index
from bdc_scripts.models.base_sql import BaseModel


class Product(BaseModel):
    __tablename__ = 'products'
    __table_args__ = (
        Index('products_general', 'type', 'datacube', 'tileid', 'start', 'end'),
        dict(schema='datastore')
    )

    id = Column(BigInteger, primary_key=True)
    datacube = Column(String(48), nullable=False)
    tileid = Column(String(16), nullable=False)
    start = Column(Date, nullable=False)
    end = Column(Date, nullable=False)
    type = Column(String(16), nullable=False, server_default=text("'SCENE'"))
    sceneid = Column(String(64), nullable=False)
    band = Column(String(16), nullable=False)
    cloud = Column(Float, nullable=False)
    processingdate = Column(DateTime)
    TL_Latitude = Column(Float)
    TL_Longitude = Column(Float)
    BR_Latitude = Column(Float)
    BR_Longitude = Column(Float)
    TR_Latitude = Column(Float)
    TR_Longitude = Column(Float)
    BL_Latitude = Column(Float)
    BL_Longitude = Column(Float)
    filename = Column(String(255), nullable=False)