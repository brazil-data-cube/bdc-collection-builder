#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Wrapper for external database on AWS."""

# 3rdparty
from bdc_db.models.base_sql import BaseModel, db
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import MetaData, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

# BDC
from .config import Config


class DatabaseWrapper(object):
    """SQLAlchemy database wrapper."""

    def __init__(self):
        """Build Database Wrapper for SQLALchemy."""
        maker = sessionmaker()
        self.session = scoped_session(maker)
        self.Model = declarative_base(metadata=MetaData())

    def initialize(self, uri: str = None):
        """Configure SQLALchemy instance scope.

        Args:
            uri - Database URI. Default is Config.SQLALCHEMY_DATABASE_URI_AWS
        """
        if uri is None:
            uri = Config.SQLALCHEMY_DATABASE_URI_AWS

        self.engine = create_engine(uri)
        self.session.configure(bind=self.engine)


db_aws = DatabaseWrapper()


def add_instance(engine, *instances):
    """Add model object into session scope.

    Args:
        engine(DatabaseWrapper or SQLAlchemy) - Database session scope
        *instances - Multiples model object
    """
    for instance in instances:
        engine.session.add(instance)


def commit(engine):
    """Persist on database engine.

    Args:
        engine (DatabaseWrapper or SQLAlchemy) - Database session scope
    """
    try:
        engine.session.commit()
    except BaseException:
        rollback(engine)

def rollback(engine):
    """Rollback database engine.

    Args:
        engine (DatabaseWrapper or SQLAlchemy) - Database session scope
    """
    engine.session.rollback()
