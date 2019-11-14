from sqlalchemy import Column, DateTime, String, Integer, CHAR
from bdc_scripts.models.base_sql import BaseModel


class Request(BaseModel):
    __tablename__ = 'Request'
    __table_args__ = dict(schema='catalogo')

    req_id = Column('ReqId', Integer, primary_key=True)
    user_id = Column('UserId', String(254), nullable=False)
    req_date = Column('ReqDate', DateTime, nullable=False)
    status_date = Column('StatusDate', DateTime, nullable=False)
    pay_date = Column('PayDate', DateTime)
    del_date = Column('DelDate', DateTime)
    priority = Column('Priority', Integer, nullable=False)
    operator = Column('Operator', String(20), nullable=False)
    address_id = Column('addressId', Integer, nullable=False)
    ip = Column('Ip', String(20))
    country = Column('Country', String(50))
    language = Column('Language', CHAR(2), nullable=False)
