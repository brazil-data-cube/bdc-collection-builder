from marshmallow import Schema, fields
from marshmallow_sqlalchemy import ModelSchema
from bdc_db.models import db, CollectionItem
from bdc_collection_builder.radcor.models import RadcorActivity, RadcorActivityHistory


class TaskSchema(Schema):
    id = fields.Integer(dump_only=True)
    status = fields.Str()
    task_id = fields.Str()
    date_done = fields.DateTime()
    traceback = fields.Str()


class CollectionItemForm(ModelSchema):
    collection_id = fields.String()
    grs_schema_id = fields.String()
    tile_id = fields.String()

    class Meta:
        model = CollectionItem
        sqla_session = db.session
        exclude = ('grs_schema', 'cube_collection', 'tile')


class HistoryForm(ModelSchema):
    status = fields.Method('dump_status')
    end = fields.Method('dump_end')

    def dump_status(self, obj):
        return obj.task.status

    def dump_end(self, obj):
        end_date = obj.task.date_done

        return str(end_date or '')

    class Meta:
        model = RadcorActivityHistory
        sqla_session = db.session
        exclude = ('activity', )


class RadcorActivityForm(ModelSchema):
    # history = fields.Nested(HistoryForm, many=True)
    last_execution = fields.Method('dump_last_execution')
    collection_id = fields.Str()

    class Meta:
        model = RadcorActivity
        sqla_session = db.session
        exclude = ('collection', 'history')

    def dump_last_execution(self, obj):
        return HistoryForm().dump(obj.history[0]) if len(obj.history) > 0 else None
