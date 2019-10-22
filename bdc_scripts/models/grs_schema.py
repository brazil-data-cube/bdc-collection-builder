from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel


class GRSSchema(BaseModel):
    __tablename__ = 'grs_schemas'

    id = Column(Integer, auto_increment=True, primary_key=True)
    description = Column(Text, nullable=False)

    tiles = relationship('Tile', backref='grs_schemas')
    collections = relationship('CubeCollection', backref='grs_schemas')