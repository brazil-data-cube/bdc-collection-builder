#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Define interface for validate request date."""


# 3rdparty
from bdc_catalog.models import Collection, Item, db
from marshmallow import (Schema, ValidationError, fields, post_load, pre_load,
                         validates_schema)
from marshmallow.validate import OneOf
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema

# Builder
from .collections.models import RadcorActivity, RadcorActivityHistory


class TaskSchema(Schema):
    """Define schema for Celery Tasks."""

    id = fields.Integer(dump_only=True)
    status = fields.Str()
    task_id = fields.Str()
    date_done = fields.DateTime()
    traceback = fields.Str()


class CollectionItemForm(SQLAlchemyAutoSchema):
    """Define schema for Collection Item."""

    collection_id = fields.String()
    grs_schema_id = fields.String()
    tile_id = fields.String()

    class Meta:
        """Define internal model handling."""

        model = Item
        sqla_session = db.session
        exclude = ('geom', 'min_convex_hull', 'tile')


class CollectionForm(SQLAlchemyAutoSchema):
    """Define schema for Collection Item."""

    class Meta:
        """Define internal model handling."""

        model = Collection
        sqla_session = db.session
        exclude = ('extent', )


class HistoryForm(SQLAlchemyAutoSchema):
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


class SimpleActivityForm(SQLAlchemyAutoSchema):
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


class TaskDispatcher(Schema):
    """Define the minimal structure for a Task."""

    type = fields.String(required=True, allow_none=False, validate=OneOf(['download', 'correction', 'publish', 'post', 'harmonization']))
    collection = fields.String(required=True, allow_none=False)
    args = fields.Dict(required=False, allow_none=False)
    tasks = fields.Nested('TaskDispatcher', required=False, allow_none=None, many=True)


class SearchImageForm(Schema):
    """Define the schema to search for images on Remote Providers."""

    dataset = fields.String(required=True, allow_none=False)
    platform = fields.String(required=False, allow_none=False)
    force = fields.Boolean(required=False, allow_none=False, default=False)
    catalog = fields.String(required=True, allow_none=False)
    tasks = fields.Nested(TaskDispatcher, required=False, allow_none=None, many=True)
    start = fields.DateTime(required=True, allow_none=False)
    end = fields.DateTime(required=True, allow_none=False)
    tags = fields.List(fields.String, allow_none=False)
    cloud = fields.Float(default=100, allow_nan=False)
    action = fields.String(required=False, validate=OneOf(['preview', 'start']), default='preview')
    w = fields.Float(allow_none=False, allow_nan=False)
    s = fields.Float(allow_none=False, allow_nan=False)
    e = fields.Float(allow_none=False, allow_nan=False)
    n = fields.Float(allow_none=False, allow_nan=False)
    scenes = fields.List(fields.String(), allow_none=False)
    tiles = fields.List(fields.String(), allow_none=False)

    @post_load
    def pre_load_dates(self, data, **kwargs) -> dict:
        """Format the parsed data and serialize the 'start' and 'end' as 'Y-m-d' string."""
        if 'start' in data:
            data['start'] = data['start'].isoformat()

        if 'end' in data:
            data['end'] = data['end'].isoformat()

        return data

    @validates_schema
    def validate_scenes(self, data, **kwargs):
        """Validate the search image form.

        Ensure that bounding box given (w, s, e, n) and scenes is not set in the same context.

        Raises:
            ValidationError When both scenes and bounding box given. It also raise error when bbox is inconsistent.
        """
        bbox_given = data.keys() >= {'w', 's', 'e', 'n'}

        if 'scenes' in data and bbox_given and 'tiles' in data:
            raise ValidationError('"scenes" and bbox ("w", "s", "e", "n") given. Please refer one of those.')

        if 'scenes' not in data and not bbox_given and 'tiles' not in data:
            raise ValidationError('Missing bbox ("w", "s", "e", "n") or "scenes" or "tiles" property.')

        if bbox_given:
            w, s, e, n = data['w'], data['s'], data['e'], data['n']

            if w > e:
                raise ValidationError('Xmin is greater than XMax')

            if s > n:
                raise ValidationError('Ymin is greater than YMax')


class CheckScenesForm(Schema):
    """Define a schema to validate CheckScenes resource."""

    catalog = fields.String(required=True, allow_none=False)
    dataset = fields.String(required=True, allow_none=False)
    catalog_kwargs = fields.Dict(required=False, allow_none=False)
    collections = fields.List(fields.String, required=True, allow_none=False)
    grid = fields.String(required=False, allow_none=False)
    tiles = fields.List(fields.String, required=False, allow_none=False)
    bbox = fields.List(fields.Float, required=False, allow_none=False, many=True)
    start_date = fields.DateTime(required=True, allow_none=False)
    end_date = fields.DateTime(required=True, allow_none=False)
    only_tiles = fields.Boolean(required=False, allow_none=False, default=False)

    @validates_schema
    def validate_form_values(self, data, **kwargs):
        """Apply minimal validation for form fields given."""
        if data.get('grid') and 'tiles' not in data:
            raise ValidationError('Missing property "tiles".')

        if 'grid' not in data and 'tiles' not in data and 'bbox' not in data:
            raise ValidationError('Missing "tiles"/"grid" or "bbox". Please refer one of.')
