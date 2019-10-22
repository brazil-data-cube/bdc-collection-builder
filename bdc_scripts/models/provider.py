from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel


class Provider(BaseModel):
    __tablename__ = 'providers'

    id = Column(Integer, auto_increment=True, primary_key=True)
    storage_type = Column(String(length=254), nullable=True)
    description = Column(Text, nullable=True)

    providers = relationship('AssetProvider', backref="providers")