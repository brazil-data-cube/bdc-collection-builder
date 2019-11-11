from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, DateTime


db = SQLAlchemy()


class BaseModel(db.Model):
    """
    Abstract class for ORM model.
    Injects both `created_at` and `updated_at` fields in table
    """

    __abstract__ = True

    created_at = Column(DateTime, default=datetime.utcnow())
    updated_at = Column(DateTime, default=datetime.utcnow(),
                                  onupdate=datetime.utcnow())

    @classmethod
    def query(cls):
        """Wraps an SQLAlchemy session query for your own model"""
        return db.session.query(cls)

    @classmethod
    def _filter(cls, **properties):
        """Filter abstraction"""
        return db.session.query(cls).filter_by(**properties)

    @classmethod
    def filter(cls, **properties):
        """
        Filter data set rows following the provided restrictions
        Provides a wrapper of SQLAlchemy session query.
        Args:
            **properties (dict) - List of properties to filter of.
        Returns:
            list of BaseModel item Retrieves the filtered rows
        """
        return cls._filter(**properties).all()

    @classmethod
    def get(cls, **restrictions):
        """
        Get one data set from database.
        Throws exception **NoResultFound** when the filter
        does not match any result.
        Args:
            **properties (dict) - List of properties to filter of.
        Returns:
            BaseModel Retrieves the base model instance
        """
        return cls._filter(**restrictions).one()

    def save(self, commit=True):
        """Save record in database"""

        with db.session.begin_nested():
            db.session.add(self)

        if not commit:
            return

        db.session.commit()

    def delete(self):
        """Delete object from database."""

        try:
            db.session.delete(self)
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            raise e