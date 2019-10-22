from geoalchemy2 import Geometry
from sqlalchemy import Column, Integer
from bdc_scripts.models.base_sql import BaseModel


class Tile(BaseModel):
    __tablename__ = 'tiles'

    id = Column(Integer, auto_increment=True, primary_key=True)
    geom_wgs84 = Column(Geometry(srid=4326), nullable=False)
    geom = Column(Geometry)
    grs_schema_id = Column(Integer, ForeignKey('grs_schemas.id'))