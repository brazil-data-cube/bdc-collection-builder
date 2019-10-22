from sqlalchemy import Column, Integer, PrimaryKeyConstraint, String, Text
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel


class AssetProvider(BaseModel):
    __tablename__ = 'asset_providers'
    # __table_args__ = (
    #     PrimaryKeyConstraint(
    #         ,
    #         col2),
    #     {}
    # )

    id = Column(Integer, auto_increment=True, primary_key=True)
    storage_info = Column(String(length=254), nullable=True)
    description = Column(Text, nullable=True)
    provider_id = Column(Integer, ForeignKey('providers.id'), nullable=True)
    cube_collection_id = Column(Integer, ForeignKey('cube_collections.id'), nullable=True)

    provider = relationship('Provider', backref='provider_assets')
