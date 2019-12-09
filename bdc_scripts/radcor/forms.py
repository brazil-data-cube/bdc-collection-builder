from marshmallow import Schema, fields
from marshmallow_sqlalchemy import ModelSchema
from bdc_db.models import db
from bdc_scripts.radcor.models import RadcorActivity, RadcorActivityHistory


class TaskSchema(Schema):
    id = fields.Integer(dump_only=True)
    status = fields.Str()
    task_id = fields.Str()
    date_done = fields.DateTime()
    traceback = fields.Str()


class HistoryForm(ModelSchema):
    task_status = fields.Method('dump_status')

    def dump_status(self, obj):
        return obj.task.status

    class Meta:
        model = RadcorActivity
        sqla_session = db.session


class RadcorActivityForm(ModelSchema):
    history = fields.Nested(HistoryForm, many=True)
    collection_id = fields.Str()

    class Meta:
        model = RadcorActivity
        sqla_session = db.session
        exclude = ('collection', )