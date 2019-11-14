from sqlalchemy import Column, DateTime, String, BigInteger, Float
from bdc_scripts.models.base_sql import BaseModel


class Product(BaseModel):
    __tablename__ = 'Products'
    __table_args__ = dict(schema='catalogo')

    id = Column('Id', BigInteger, primary_key=True)
    dataset = Column('Dataset', String(50), nullable=False)
    type = Column('Type', String(20), nullable=False)
    processing_date = Column('ProcessingDate', DateTime)
    geometric_processing = Column('GeometricProcessing', String(20), nullable=False)
    radiometric_processing = Column('RadiometricProcessing', String(20), nullable=False)
    sceneid = Column('SceneId', String(64), nullable=False)
    band = Column('Band', String(20), nullable=False)
    resolution = Column('Resolution', Float)
    filename = Column('Filename', String(255), nullable=False)
