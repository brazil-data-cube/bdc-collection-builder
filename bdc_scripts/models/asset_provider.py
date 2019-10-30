from sqlalchemy import Column, Float, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel


class AssetProvider(BaseModel):
    __tablename__ = 'asset_providers'

    cube_collection = Column(ForeignKey('cube_collections.id'), primary_key=True, nullable=False)
    provider = Column(ForeignKey('providers.id'), primary_key=True, nullable=False)
    storage_info = Column(String(32))
    description = Column(String(64))

    cube_collection1 = relationship('CubeCollection')
    provider1 = relationship('Provider')