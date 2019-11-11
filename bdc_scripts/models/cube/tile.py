from geoalchemy2 import Geometry
from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel


class Tile(BaseModel):
    __tablename__ = 'tiles'

    id = Column(String(20), primary_key=True, nullable=False, unique=True)
    grs_schema = Column(ForeignKey('grs_schemas.id'), primary_key=True, nullable=False)
    geom_wgs84 = Column(Geometry)
    geom = Column(Geometry)

    grs_schema_instance = relationship('GrsSchema')