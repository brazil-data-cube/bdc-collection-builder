from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel


class BandComposition(BaseModel):
    __tablename__ = 'band_compositions'

    cube_collection = Column(ForeignKey('cube_collections.id'), primary_key=True, nullable=False)
    band = Column(ForeignKey('bands.id'), primary_key=True, nullable=False)
    product = Column(String(16), primary_key=True, nullable=False, unique=True)
    product_band = Column(String(16), primary_key=True, nullable=False, unique=True)
    description = Column(String(64))

    band_instance = relationship('Band')
    cube_collection_instance = relationship('CubeCollection')