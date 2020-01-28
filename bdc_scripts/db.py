from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from bdc_db.models.base_sql import db, BaseModel
from bdc_scripts.config import Config


class DatabaseWrapper(object):
    def __init__(self):
        maker = sessionmaker()
        self.session = scoped_session(maker)
        self.Model = declarative_base(metadata=MetaData())

    def initialize(self, uri=None):
        if uri is None:
            uri = Config.SQLALCHEMY_DATABASE_URI_AWS

        self.engine = create_engine(uri)
        self.session.configure(bind=self.engine)


db_aws = DatabaseWrapper()


def add_instance(engine, *instances):
    for instance in instances:
        engine.session.add(instance)

def commit(engine):
    try:
        engine.session.commit()
    except BaseException:
        rollback(engine)

def rollback(engine):
    engine.session.rollback()
