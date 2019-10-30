from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel
from bdc_scripts.models.cube_tile import CubeTile


class CubeCollection(BaseModel):
    __tablename__ = 'cube_collections'

    id = Column(String(20), primary_key=True)
    spatial_resolution_schema = Column(ForeignKey('spatial_resolution_schemas.id'), nullable=False)
    temporal_composition_schema = Column(ForeignKey('temporal_composition_schemas.id'), nullable=False)
    raster_chunk_schema = Column(ForeignKey('raster_chunk_schemas.id'), nullable=False)
    grs_schema = Column(ForeignKey('grs_schemas.id'), nullable=False)
    version = Column(String(16), nullable=False)
    description = Column(String(64), nullable=False)

    # Associations
    grs_schema_instance = relationship('GrsSchema')
    raster_chunk_schema_instance = relationship('RasterChunkSchema')
    spatial_resolution_schema_instance = relationship('SpatialResolutionSchema')
    temporal_composition_schema_instance = relationship('TemporalCompositionSchema')