from sqlalchemy import Column, String
from bdc_scripts.models.base_sql import BaseModel


class Qlook(BaseModel):
    __tablename__ = 'Qlook'
    __table_args__ = dict(schema='catalogo')

    SceneId = Column('SceneId', String(64), primary_key=True, index=True)
    QLfilename = Column('QLfilename', String(255), nullable=False, unique=True)
