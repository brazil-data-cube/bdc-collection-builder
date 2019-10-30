from sqlalchemy import Column, Integer, Numeric, \
                       String, Text
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel


class RasterChunkSchema(BaseModel):
    __tablename__ = 'raster_chunk_schemas'

    id = Column(Integer, auto_increment=True, primary_key=True)
    raster_size_x = Column(Numeric)
    raster_size_y = Column(Numeric)
    raster_size_t = Column(Numeric)
    # Make association
    collections = relationship("CubeCollection")