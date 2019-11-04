from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel


class CompositeFunction(BaseModel):
    __tablename__ = 'composite_functions'

    id = Column(String(20), primary_key=True, nullable=False, unique=True)
    cube_collection = Column(ForeignKey('cube_collections.id'), primary_key=True, nullable=False)
    description = Column(String(64))

    cube_collection_instance = relationship('CubeCollection')