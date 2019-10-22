from sqlalchemy import Column, Integer, String, Text, ForeignKey
from bdc_scripts.models.base_sql import BaseModel


class CompositeFunction(BaseModel):
    __tablename__ = 'composite_functions'

    id = Column(Integer, auto_increment=True, primary_key=True)
    version = Column(String(length=25), nullable=True)
    description = Column(Text, nullable=True)

    cube_collection_id = Column(
        Integer,
        ForeignKey('cube_collections.id')
    )