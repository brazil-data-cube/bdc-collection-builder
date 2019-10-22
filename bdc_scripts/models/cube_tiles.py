from sqlalchemy import Column, ForeignKey, Integer, String, Text
from bdc_scripts.models.base_sql import BaseModel


class CubeTile(BaseModel):
    __tablename__ = 'cube_tiles'

    cube_collection_id = Column(Integer, ForeignKey('cube_collections.id'), nullable=True)
    # grs_schema_id = Column(Integer, ForeignKey('grs_schemas.id'), nullable=True)
    tile_id = Column(Integer, ForeignKey('tiles.id'), nullable=True)