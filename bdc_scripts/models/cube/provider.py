from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel


class Provider(BaseModel):
    __tablename__ = 'providers'

    id = Column(String(20), primary_key=True)
    name = Column(String(64), nullable=False, unique=True)
    storage_type = Column(String(16), nullable=False)
    description = Column(String(64), nullable=False)
