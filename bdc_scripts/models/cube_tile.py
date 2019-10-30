from sqlalchemy import Column, ForeignKey, Integer, Table
from bdc_scripts.models.base_sql import db


CubeTile = Table(
    'cube_tiles',
    db.metadata,
    Column('cube_collection_id', Integer, ForeignKey('cube_collections.id'), nullable=True),
    Column('tile_id', Integer, ForeignKey('tiles.id'), nullable=True)
)