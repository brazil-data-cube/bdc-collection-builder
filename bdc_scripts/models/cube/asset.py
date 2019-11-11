from sqlalchemy import Column, ForeignKey, Integer, Sequence
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel


class Asset(BaseModel):
    __tablename__ = 'assets'

    id = Column(Integer, Sequence('assets_id_seq'), primary_key=True, nullable=False, unique=True)
    cube_collection = Column(ForeignKey('cube_collections.id'), primary_key=True, nullable=False)
    band = Column(ForeignKey('bands.id'))
    grs_schema = Column(ForeignKey('grs_schemas.id'))
    tile = Column(ForeignKey('tiles.id'))
    cube_item = Column(ForeignKey('cube_items.id'))

    band_instance = relationship('Band')
    cube_collection_instance = relationship('CubeCollection')
    cube_item_instance = relationship('CubeItem')
    grs_schema_instance = relationship('GrsSchema')
    tile_instance = relationship('Tile')