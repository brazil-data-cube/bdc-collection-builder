#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Define interface for validate request date."""


# 3rdparty
from marshmallow import Schema, fields
from marshmallow_sqlalchemy import ModelSchema
from bdc_db.models import db, CollectionItem
# Builder
from .models import RadcorActivity, RadcorActivityHistory


class TaskSchema(Schema):
    """Define schema for Celery Tasks."""

    id = fields.Integer(dump_only=True)
    status = fields.Str()
    task_id = fields.Str()
    date_done = fields.DateTime()
    traceback = fields.Str()


class CollectionItemForm(ModelSchema):
    """Define schema for Collection Item."""

    collection_id = fields.String()
    grs_schema_id = fields.String()
    tile_id = fields.String()

    class Meta:
        """Define internal model handling."""

        model = CollectionItem
        sqla_session = db.session
        exclude = ('grs_schema', 'cube_collection', 'tile')


class HistoryForm(ModelSchema):
    """Define schema for task execution history."""

    status = fields.Method('dump_status')
    end = fields.Method('dump_end')

    def dump_status(self, obj):
        """Dump celery status into schema."""
        return obj.task.status

    def dump_end(self, obj):
        """Dump celery execution date_done on schema."""
        end_date = obj.task.date_done

        return str(end_date or '')

    class Meta:
        """Define internal model handling."""

        model = RadcorActivityHistory
        sqla_session = db.session
        exclude = ('activity', )


class SimpleActivityForm(ModelSchema):
    """Define schema for Brazil Data Cube Collection Builder Activity."""

    collection_id = fields.Str()

    class Meta:
        """Define internal model handling."""

        model = RadcorActivity
        sqla_session = db.session
        exclude = ('collection', 'history')


class RadcorActivityForm(SimpleActivityForm):
    """Define schema for Brazil Data Cube Collection Builder Activity."""

    last_execution = fields.Method('dump_last_execution')

    def dump_last_execution(self, obj):
        """Dump last task execution."""
        return HistoryForm().dump(obj.history[0]) if len(obj.history) > 0 else None
