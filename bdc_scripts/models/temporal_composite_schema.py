from sqlalchemy import Column, Integer, Numeric, \
                       String, Text
from bdc_scripts.models.base_sql import BaseModel


class TemporalCompositeSchema(BaseModel):
    __tablename__ = 'temporal_composite_schemas'

    id = Column(Integer, auto_increment=True, primary_key=True)
    temporal_schema = Column(String(length=50))
    temporal_composite_t = Column(Numeric)
    temporal_composite_unit = Column(String(length=20))
    # Make association
    collections = relationship("CubeCollection")