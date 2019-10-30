from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel


class Cube(BaseModel):
    __tablename__ = 'cubes'

    id = Column(String(20), primary_key=True)
    cube_collection = Column(ForeignKey('cube_collections.id'))
    provider = Column(ForeignKey('providers.id'))
    composite_function = Column(ForeignKey('composite_functions.id'))
    oauth_info = Column(String(16))
    description = Column(String(64))

    composite_function_instance = relationship('CompositeFunction')
    cube_collection_instance = relationship('CubeCollection')
    provider_instance = relationship('Provider')
