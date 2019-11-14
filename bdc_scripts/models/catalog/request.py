from sqlalchemy import Column, DateTime, String, Integer, CHAR
from bdc_scripts.models.base_sql import BaseModel


class Request(BaseModel):
    __tablename__ = 'Request'
    __table_args__ = dict(schema='catalogo')

    ReqId = Column('ReqId', Integer, primary_key=True)
    UserId = Column('UserId', String(254), nullable=False)
    ReqDate = Column('ReqDate', DateTime, nullable=False)
    StatusDate = Column('StatusDate', DateTime, nullable=False)
    PayDate = Column('PayDate', DateTime)
    DelDate = Column('DelDate', DateTime)
    Priority = Column('Priority', Integer, nullable=False)
    Operator = Column('Operator', String(20), nullable=False)
    addressId = Column('addressId', Integer, nullable=False)
    Ip = Column('Ip', String(20))
    Country = Column('Country', String(50))
    Language = Column('Language', CHAR(2), nullable=False)
