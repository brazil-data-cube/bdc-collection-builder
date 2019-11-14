from sqlalchemy import Column, Date, DateTime, String, Integer, Float, text, Index, CHAR, SmallInteger
from bdc_scripts.models.base_sql import BaseModel


class Scene(BaseModel):
    __tablename__ = 'Scene'
    __table_args__ = (
        Index('Scene_idx1', 'Satellite', 'Sensor'),
        Index('Scene_idx6', 'CloudCoverQ1', 'CloudCoverQ2', 'CloudCoverQ3', 'CloudCoverQ4'),
        dict(schema='catalogo')
    )

    SceneId = Column('SceneId', String(64), primary_key=True, server_default=text("''"))
    IdRunMode = Column('IdRunMode', Integer)
    Satellite = Column('Satellite', String(50))
    Sensor = Column('Sensor', String(6), nullable=False, index=True)
    Path = Column('Path', String(11), index=True)
    Row = Column('Row', String(11), index=True)
    Date = Column('Date', Date, index=True)
    Orbit = Column('Orbit', Integer)
    CenterLatitude = Column('CenterLatitude', Float)
    CenterLongitude = Column('CenterLongitude', Float)
    TL_Latitude = Column('TL_Latitude', Float)
    tl_longitude = Column('TL_Longitude', Float)
    BR_Latitude = Column('BR_Latitude', Float)
    BR_Longitude = Column('BR_Longitude', Float)
    TR_Latitude = Column('TR_Latitude', Float)
    TR_Longitude = Column('TR_Longitude', Float)
    BL_Latitude = Column('BL_Latitude', Float)
    BL_Longitude = Column('BL_Longitude', Float)
    CenterTime = Column('CenterTime', Float(asdecimal=True))
    StartTime = Column('StartTime', Float(asdecimal=True))
    StopTime = Column('StopTime', Float(asdecimal=True))
    ImageOrientation = Column('ImageOrientation', Float)
    SyncLosses = Column('SyncLosses', Integer)
    num_miss_swath = Column('NumMissSwath', Integer)
    NumMissSwath = Column('PerMissSwath', Float)
    BitSlips = Column('BitSlips', Integer)
    CloudCoverQ1 = Column('CloudCoverQ1', Integer)
    CloudCoverQ2 = Column('CloudCoverQ2', Integer)
    CloudCoverQ3 = Column('CloudCoverQ3', Integer)
    CloudCoverQ4 = Column('CloudCoverQ4', Integer)
    CloudCoverMethod = Column('CloudCoverMethod', CHAR(1))
    Grade = Column('Grade', Float)
    IngestDate = Column('IngestDate', DateTime)
    Deleted = Column('Deleted', SmallInteger, nullable=False)
    Dataset = Column('Dataset', String(50))
    ExportDate = Column('ExportDate', DateTime)
    AuxPath = Column('AuxPath', String(11))
