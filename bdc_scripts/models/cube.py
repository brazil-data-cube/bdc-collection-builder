from sqlalchemy import Column, Integer, String, Text, ForeignKey
from bdc_scripts.models.base_sql import BaseModel


class Cube(BaseModel):
    __tablename__ = 'cubes'

    id = Column(Integer, auto_increment=True, primary_key=True)
    oauth_info = Column(String(length=254), nullable=True)
    description = Column(Text, nullable=True)

    # cube_collection_id = Column(Integer, ForeignKey('cube_collections.id'), nullable=True)
    composite_function_id = Column(Integer, ForeignKey('composite_functions.id'), nullable=True)
    grs_schema_id = Column(Integer, ForeignKey('grs_schemas.id'), nullable=True)
    tile_id = Column(Integer, ForeignKey('tiles.id'), nullable=True)