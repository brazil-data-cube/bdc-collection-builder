from sqlalchemy import Column, ForeignKey
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel


# CubeTile = Table(
#     'cube_tiles',
#     db.metadata,
#     Column('cube_collection_id', Integer, ForeignKey('cube_collections.id'), nullable=True),
#     Column('tile_id', Integer, ForeignKey('tiles.id'), nullable=True)
# )

class CubeTile(BaseModel):
    __tablename__ = 'cube_tiles'

    cube_collection = Column(ForeignKey('cube_collections.id'), primary_key=True, nullable=False)
    grs_schema = Column(ForeignKey('grs_schemas.id'), primary_key=True, nullable=False)
    tile = Column(ForeignKey('tiles.id'), primary_key=True, nullable=False)

    cube_collection_instance = relationship('CubeCollection')
    grs_schema_instance = relationship('GrsSchema')
    tile_instance = relationship('Tile')
