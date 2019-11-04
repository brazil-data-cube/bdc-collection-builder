from sqlalchemy import Column, Float, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel


class Band(BaseModel):
    __tablename__ = 'bands'

    id = Column(String(20), primary_key=True, nullable=False, unique=True)
    cube_collection = Column(ForeignKey('cube_collections.id'), primary_key=True, nullable=False)
    min = Column(Float)
    max = Column(Float)
    fill = Column(Integer)
    scale = Column(String(16))
    commom_name = Column(String(16))
    data_type = Column(String(16))
    mime_type = Column(String(16))
    description = Column(String(64))

    cube_collection_instance = relationship('CubeCollection')