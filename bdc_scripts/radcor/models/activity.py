from celery.backends.database import Task
from sqlalchemy import Column, DateTime, Integer, String, Time, or_, ForeignKey
from sqlalchemy.orm import relationship
from bdc_scripts.models.base_sql import db, BaseModel


class RadcorActivity(BaseModel):
    __tablename__ = 'activities'
    __table_args__ = dict(schema='radcor')

    id = Column('id', Integer, nullable=False, unique=True, primary_key=True)
    app = Column('app', String(64), nullable=False)
    sceneid = Column('sceneid', String(64), nullable=False)
    satellite = Column('satellite', String(8))
    priority = Column('priority', Integer)
    status = Column('status', String(16))
    link = Column('link', String(256))
    file = Column('file', String(128))
    start = Column('start', DateTime)
    end = Column('end', DateTime)
    elapsed = Column('elapsed', Time)
    retcode = Column('retcode', Integer)
    message = Column('message', String(512))
    task_id = Column(ForeignKey(Task.id), primary_key=True, nullable=False)

    task = relationship(Task, uselist=False)

    @classmethod
    def get_by_task_id(cls, task_id: str):
        return cls.query().filter(cls.task.has(task_id=task_id)).one()

    @classmethod
    def reset_status(cls, id=None):
        """
        Reset inconsistent activities to NOTDONE

        Args:
            id (int or None) - Activity Id. Default is None, which represents all

        Returns:
            list of RadcorActivity
        """

        # if id is not None:
        #     where = cls.id == id
        # else:
        #     where = None

        with db.session.begin_nested():
            if id is not None:
                where = cls.id == id
            else:
                where = or_(
                    cls.status == 'ERROR',
                    cls.status == 'DOING',
                    cls.status == 'SUSPEND'
                )

            elements = cls.query().filter(where)

            elements.update(dict(status='NOTDONE'))

        db.session.commit()

        return elements.all()

    @classmethod
    def is_started_or_done(cls, sceneid: str):
        return cls.query().filter(cls.sceneid == sceneid).all()
