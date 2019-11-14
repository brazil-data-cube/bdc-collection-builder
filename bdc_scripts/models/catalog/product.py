from sqlalchemy import Column, DateTime, String, BigInteger, Float
from bdc_scripts.models.base_sql import BaseModel


class Product(BaseModel):
    __tablename__ = 'Products'
    __table_args__ = dict(schema='catalogo')

    Id = Column('Id', BigInteger, primary_key=True)
    Dataset = Column('Dataset', String(50), nullable=False)
    Type = Column('Type', String(20), nullable=False)
    ProcessingDate = Column('ProcessingDate', DateTime)
    GeometricProcessing = Column('GeometricProcessing', String(20), nullable=False)
    RadiometricProcessing = Column('RadiometricProcessing', String(20), nullable=False)
    SceneId = Column('SceneId', String(64), nullable=False)
    Band = Column('Band', String(20), nullable=False)
    Resolution = Column('Resolution', Float)
    Filename = Column('Filename', String(255), nullable=False)
