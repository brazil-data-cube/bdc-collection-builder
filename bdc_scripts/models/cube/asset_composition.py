from sqlalchemy import Column, Date, ForeignKey, Text
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel


class AssetComposition(BaseModel):
    __tablename__ = 'asset_compositions'

    cube_collection = Column(ForeignKey('cube_collections.id'), primary_key=True, nullable=False)
    asset = Column(ForeignKey('assets.id'), primary_key=True, nullable=False)
    product = Column(ForeignKey('band_compositions.product'), primary_key=True, nullable=False)
    product_band = Column(ForeignKey('band_compositions.product_band'), primary_key=True, nullable=False)
    reference_date = Column(Date, primary_key=True, nullable=False)
    file_path = Column(Text)

    asset_instance = relationship('Asset')
    cube_collection_instance = relationship('CubeCollection')
    band_composition_product = relationship('BandComposition', primaryjoin='AssetComposition.product == BandComposition.product')
    band_composition_product_band = relationship('BandComposition', primaryjoin='AssetComposition.product_band == BandComposition.product_band')