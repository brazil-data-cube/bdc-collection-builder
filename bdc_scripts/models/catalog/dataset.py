from sqlalchemy import Column, String, Integer
from bdc_scripts.models.base_sql import BaseModel


class Dataset(BaseModel):
    __tablename__ = 'Dataset'
    __table_args__ = dict(schema='catalogo')

    Id = Column(Integer, primary_key=True)
    Name = Column(String(50), nullable=False)
    Description = Column(String(512), nullable=False)
