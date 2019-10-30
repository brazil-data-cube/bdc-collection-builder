from sqlalchemy import Column, ForeignKey, Integer, Numeric, \
                       String, Text
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel
from bdc_scripts.models.cube_tile import CubeTile


class CubeCollection(BaseModel):
    __tablename__ = 'cube_collections'

    id = Column(Integer, auto_increment=True, primary_key=True)
    grs_schema_id = Column(Integer, ForeignKey('grs_schemas.id'))
    spatial_resolution_schema_id = Column(
        Integer,
        ForeignKey('spatial_resolution_schemas.id')
    )
    temporal_composite_schema_id = Column(
        Integer,
        ForeignKey('temporal_composite_schemas.id')
    )
    raster_chunk_schema_id = Column(
        Integer,
        ForeignKey('raster_chunk_schemas.id')
    )

    composite_functions = relationship('CompositeFunction')