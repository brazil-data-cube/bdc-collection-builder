from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel


class AssetLink(BaseModel):
    __tablename__ = 'asset_links'

    cube_collection = Column(String(20), primary_key=True, nullable=False)
    provider = Column(String(20), primary_key=True, nullable=False)
    asset = Column(ForeignKey('assets.id'), primary_key=True, nullable=False)
    file_path = Column(String(64))

    asset_instance = relationship('Asset')