from sqlalchemy import Column, Integer, Numeric, \
                       String, Text
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel


class SpatialResolutionSchema(BaseModel):
    __tablename__ = 'spatial_resolution_schemas'

    id = Column(Integer, auto_increment=True, primary_key=True)
    resolution_x = Column(Numeric, nullable=False)
    resolution_y = Column(Numeric, nullable=False)
    resolution_unit = Column(String(length='20'), nullable=False)
    # Make association
    collections = relationship("CubeCollection")