from sqlalchemy import Column, Date, ForeignKey, Integer, Text, Sequence
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel


class CubeItem(BaseModel):
    __tablename__ = 'cube_items'

    id = Column(Integer, Sequence('cube_items_id_seq'), primary_key=True, nullable=False, unique=True)
    cube_collection = Column(ForeignKey('cube_collections.id'), primary_key=True, nullable=False)
    grs_schema = Column(ForeignKey('grs_schemas.id'), primary_key=True, nullable=False)
    tile = Column(ForeignKey('tiles.id'), primary_key=True, nullable=False)
    composite_function = Column(ForeignKey('composite_functions.id'), primary_key=True, nullable=False)
    item_date = Column(Date, primary_key=True, nullable=False)
    composite_start = Column(Date, nullable=False)
    composite_end = Column(Date)
    quicklook = Column(Text)

    composite_function_instance = relationship('CompositeFunction')
    cube_collection_instance = relationship('CubeCollection')
    grs_schema_instance = relationship('GrsSchema')
    tile_instance = relationship('Tile')