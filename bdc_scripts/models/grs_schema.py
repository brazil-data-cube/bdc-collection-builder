from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import BaseModel


class GrsSchema(BaseModel):
    __tablename__ = 'grs_schemas'

    id = Column(String(20), primary_key=True)
    description = Column(String(64), nullable=False)