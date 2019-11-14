from sqlalchemy import Column, Date, DateTime, String, Integer, Float, text, Index, CHAR, SmallInteger
from bdc_scripts.models.base_sql import BaseModel


class Scene(BaseModel):
    __tablename__ = 'Scene'
    __table_args__ = (
        Index('Scene_idx1', 'Satellite', 'Sensor'),
        Index('Scene_idx6', 'CloudCoverQ1', 'CloudCoverQ2', 'CloudCoverQ3', 'CloudCoverQ4'),
        dict(schema='catalogo')
    )

    sceneid = Column('SceneId', String(64), primary_key=True, server_default=text("''"))
    id_run_mode = Column('IdRunMode', Integer)
    satellite = Column('Satellite', String(50))
    sensor = Column('Sensor', String(6), nullable=False, index=True)
    path = Column('Path', String(11), index=True)
    row = Column('Row', String(11), index=True)
    date = Column('Date', Date, index=True)
    orbit = Column('Orbit', Integer)
    center_latitude = Column('CenterLatitude', Float)
    center_longitude = Column('CenterLongitude', Float)
    tl_latitude = Column('TL_Latitude', Float)
    tl_longitude = Column('TL_Longitude', Float)
    br_latitude = Column('BR_Latitude', Float)
    br_longitude = Column('BR_Longitude', Float)
    tr_latitude = Column('TR_Latitude', Float)
    tr_longitude = Column('TR_Longitude', Float)
    bl_latitude = Column('BL_Latitude', Float)
    bl_longitude = Column('BL_Longitude', Float)
    center_time = Column('CenterTime', Float(asdecimal=True))
    start_time = Column('StartTime', Float(asdecimal=True))
    stop_time = Column('StopTime', Float(asdecimal=True))
    image_orientation = Column('ImageOrientation', Float)
    sync_losses = Column('SyncLosses', Integer)
    num_miss_swath = Column('NumMissSwath', Integer)
    per_miss_swath = Column('PerMissSwath', Float)
    bit_slips = Column('BitSlips', Integer)
    cloud_cover_Q1 = Column('CloudCoverQ1', Integer)
    cloud_cover_Q2 = Column('CloudCoverQ2', Integer)
    cloud_cover_Q3 = Column('CloudCoverQ3', Integer)
    cloud_cover_Q4 = Column('CloudCoverQ4', Integer)
    cloud_cover_method = Column('CloudCoverMethod', CHAR(1))
    grade = Column('Grade', Float)
    ingest_date = Column('IngestDate', DateTime)
    deleted = Column('Deleted', SmallInteger, nullable=False)
    dataset = Column('Dataset', String(50))
    export_date = Column('ExportDate', DateTime)
    aux_path = Column('AuxPath', String(11))
