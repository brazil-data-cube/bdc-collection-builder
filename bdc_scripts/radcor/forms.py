from marshmallow import Schema, fields
from marshmallow_sqlalchemy import ModelSchema
from bdc_scripts.models import db
from bdc_scripts.radcor.models import RadcorActivity


class TaskSchema(Schema):
    id = fields.Integer(dump_only=True)
    status = fields.Str()
    task_id = fields.Str()
    date_done = fields.DateTime()
    traceback = fields.Str()


class RadcorActivityForm(ModelSchema):
    task = fields.Nested(TaskSchema, many=False)

    class Meta:
        model = RadcorActivity
        sqla_session = db.session