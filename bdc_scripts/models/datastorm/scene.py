from sqlalchemy import Column, Date, String, BigInteger, Float, text, Index, SmallInteger
from bdc_scripts.models.base_sql import BaseModel


class Scene(BaseModel):
    __tablename__ = 'scenes'
    __table_args__ = (
        Index('scenes_general', 'datacube', 'tileid', 'start', 'end', 'band'),
        dict(schema='datastorm')
    )

    id = Column(BigInteger, primary_key=True)
    datacube = Column(String(32), nullable=False)
    tileid = Column(String(16), nullable=False)
    start = Column(Date, nullable=False)
    end = Column(Date, nullable=False)
    type = Column(String(16), nullable=False, server_default=text("'SCENE'"))
    dataset = Column(String(16), nullable=False)
    sceneid = Column(String(64), nullable=False)
    band = Column(String(16), nullable=False)
    pathrow = Column(String(16), nullable=False)
    date = Column(Date, nullable=False)
    cloud = Column(Float, nullable=False)
    resolution = Column(Float, nullable=False)
    cloudratio = Column(Float, nullable=False)
    clearratio = Column(Float, nullable=False)
    efficacy = Column(Float, nullable=False)
    link = Column(String(256), nullable=False)
    file = Column(String(256))
    warped = Column(String(256), nullable=False)
    enabled = Column(SmallInteger, nullable=False, index=True, server_default=text("1"))