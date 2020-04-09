#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Define base interface for Celery Tasks."""


# Python Native
from datetime import datetime
from os import path as resource_path
import glob
import logging
# 3rdparty
from celery.backends.database import Task
from sqlalchemy import or_, func, cast, Date
from werkzeug.exceptions import BadRequest
# Builder
from bdc_db.models import db, Collection, CollectionTile
from bdc_collection_builder.config import Config
from bdc_collection_builder.db import db_aws
from .forms import SimpleActivityForm
from .models import RadcorActivity, RadcorActivityHistory
from .utils import dispatch, get_landsat_scenes, get_sentinel_scenes, get_or_create_model

# Consts
CLOUD_DEFAULT = 90
DESTINATION_DIR = Config.DATA_DIR


class RadcorBusiness:
    """Define an interface for handling entire module business."""

    @classmethod
    def start(cls, activity):
        """Dispatch the celery tasks."""
        return dispatch(activity)

    @classmethod
    def restart(cls, ids=None, status=None, activity_type=None, sceneid=None, collection_id=None, action=None):
        """Restart celery task execution.

        Args:
            ids - List of Activity ID
            status - Filter by task status
            activity_type - Filter by activity type

        Returns:
            Affected activities
        """
        restrictions = []

        if ids:
            restrictions.append(RadcorActivity.id.in_(ids))

        if status:
            restrictions.append(RadcorActivityHistory.task.has(status=status))

        if activity_type:
            restrictions.append(RadcorActivity.activity_type == activity_type)

        if collection_id:
            restrictions.append(RadcorActivity.collection_id == collection_id)

        if sceneid:
            if collection_id is None or activity_type is None:
                raise BadRequest('The parameters "collection_id" and "activity_type" are required to search by sceneid.')

            scenes = sceneid.split(',') if isinstance(sceneid, str) else sceneid
            restrictions.append(RadcorActivity.sceneid.in_(scenes))

        if len(restrictions) == 0:
            raise BadRequest('Invalid restart. You must provide query restriction such "ids", "activity_type" or "status"')

        activities = db.session.query(RadcorActivity).filter(*restrictions).all()

        # Define a start wrapper in order to preview or start activity
        start_activity = cls.start if str(action).lower() == 'start' else lambda _: _

        serialized_activities = SimpleActivityForm().dump(activities, many=True)

        for activity in serialized_activities:
            start_activity(activity)

        return serialized_activities

    @classmethod
    def create_tile(cls, grs, tile, collection, engine=db):
        """Create tile on database."""
        with engine.session.begin_nested():
            restriction = dict(
                grs_schema_id=grs,
                tile_id=tile,
                collection_id=collection
            )

            _, _ = get_or_create_model(CollectionTile, defaults=restriction, engine=engine, **restriction)

        engine.session.commit()

    @classmethod
    def create_activity(cls, activity):
        """Persist an activity on database."""
        with db.session.begin_nested():
            where = dict(
                sceneid=activity['sceneid'],
                activity_type=activity['activity_type'],
                collection_id=activity['collection_id']
            )

            _, created = get_or_create_model(RadcorActivity, defaults=activity, **where)

        if created:
            db.session.commit()
        else:
            db.session.rollback()

        return created


    @classmethod
    def radcor(cls, args: dict):
        """Search for Landsat/Sentinel Images and dispatch download task."""
        args.setdefault('limit', 299)
        args.setdefault('cloud', CLOUD_DEFAULT)
        args['tileid'] = 'notile'
        args['satsen'] = args['satsen'].split(',') if 'satsen' in args else ['S2']
        args['start'] = args.get('start')
        args['end'] = args.get('end')

        # Get bbox
        w = float(args['w'])
        e = float(args['e'])
        s = float(args['s'])
        n = float(args['n'])

        # Get the requested period to be processed
        rstart = args['start']
        rend   = args['end']

        sat = args['satsen']
        cloud = float(args['cloud'])
        limit = args['limit']
        action = args['action']
        do_harmonization = (args['harmonize'].lower() == 'true') if 'harmonize' in args else False

        scenes = {}
        if 'LC8' in sat or 'LC8SR' in sat:
            # result = developmentSeed(w,n,e,s,rstart,rend,cloud,limit)
            result = get_landsat_scenes(w,n,e,s,rstart,rend,cloud,limit)
            scenes.update(result)
            for id in result:
                scene = result[id]
                sceneid = scene['sceneid']
                # Check if this scene is already in Repository
                cc = sceneid.split('_')
                yyyymm = cc[3][:4]+'-'+cc[3][4:6]
                tileid = cc[2]
                # Find LC08_L1TP_218069_20180706_20180717_01_T1.png
                base_dir = resource_path.join(DESTINATION_DIR, 'Repository/Archive/LC8')

                activity = dict(
                    collection_id='LC8DN',
                    activity_type='downloadLC8',
                    tags=args.get('tags', []),
                    sceneid=sceneid,
                    scene_type='SCENE',
                    args=dict(
                        link=scene['link'],
                        file=base_dir,
                        satellite='LC8',
                        cloud=scene.get('cloud'),
                        harmonize=do_harmonization
                    )
                )

                if not cls.create_activity(activity):
                    logging.warning('radcor - activity already done {}'.format(activity['sceneid']))
                    continue

                scene['status'] = 'NOTDONE'

                tile = '{}{}'.format(scene['path'], scene['row'])
                RadcorBusiness.create_tile('WRS2', tile, 'LC8DN', engine=db)
                RadcorBusiness.create_tile('WRS2', tile, 'LC8SR', engine=db)
                if do_harmonization:
                    RadcorBusiness.create_tile('WRS2', tile, 'LC8NBAR', engine=db)
                RadcorBusiness.create_tile('WRS2', tile, 'LC8SR', engine=db_aws)
                if do_harmonization:
                    RadcorBusiness.create_tile('WRS2', tile, 'LC8NBAR', engine=db_aws)

                if action == 'start':
                    cls.start(activity)

        if 'S2' in sat or 'S2SR_SEN28' in sat:
            result = get_sentinel_scenes(w,n,e,s,rstart,rend,cloud,limit)
            scenes.update(result)
            for id in result:
                scene = result[id]
                sceneid = scene['sceneid']
                # Check if this scene is already in Repository as Level 2A
                cc = sceneid.split('_')
                yyyymm = cc[2][:4]+'-'+cc[2][4:6]
                # Output product dir
                base_dir = resource_path.join(DESTINATION_DIR, 'Repository/Archive/S2_MSI')
                productdir = resource_path.join(base_dir, '{}/'.format(yyyymm))

                activity = dict(
                    collection_id='S2TOA',
                    activity_type='downloadS2',
                    tags=args.get('tags', []),
                    sceneid=sceneid,
                    scene_type='SCENE',
                    args=dict(
                        link=scene['link'],
                        file=base_dir,
                        satellite='S2',
                        cloud=scene.get('cloud'),
                        harmonize=do_harmonization
                    )
                )

                if not cls.create_activity(activity):
                    logging.warning('radcor - activity already done {}'.format(sceneid))
                    continue

                RadcorBusiness.create_tile('MGRS', scene.get('pathrow', scene.get('tileid')), 'S2TOA', engine=db)
                RadcorBusiness.create_tile('MGRS', scene.get('pathrow', scene.get('tileid')), 'S2SR_SEN28', engine=db)
                RadcorBusiness.create_tile('MGRS', scene.get('pathrow', scene.get('tileid')), 'S2SR_SEN28', engine=db_aws)
                scene['status'] = 'NOTDONE'

                scenes[id] = scene

                if action == 'start':
                    cls.start(activity)

        return scenes

    @classmethod
    def list_activities(cls, args: dict):
        """List task activities from database."""
        filters = []
        if args.get('scene_id'):
            filters.append(RadcorActivity.sceneid == args['scene_id'])
        if args.get('collection'):
            filters.append(RadcorActivity.collection_id == args['collection'])
        if args.get('type'):
            filters.append(RadcorActivity.activity_type.contains(args['type']))
        if args.get('period'):
            dates = args['period'].split('/')
            if len(dates) != 2:
                raise BadRequest('Incorrect dates! Format: YYYY-mm-dd/YYYY-mm-dd')
            filters.append(RadcorActivity.history.any(
                    RadcorActivityHistory.start >= datetime.strptime(dates[0], '%Y-%m-%d')))
            filters.append(RadcorActivity.history.any(
                    RadcorActivityHistory.start <= datetime.strptime(dates[1]+' 23:59', '%Y-%m-%d %H:%M')))

        activities = RadcorActivity.query().filter(*filters)
        return activities

    @classmethod
    def count_activities(cls, args: dict):
        """Count grouped by status on database."""
        filters = []
        if args.get('start_date'): filters.append(RadcorActivityHistory.start >= '{}T00:00'.format(args['start_date']))
        if args.get('last_date'): filters.append(RadcorActivityHistory.start <= '{}T23:59'.format(args['last_date']))
        if args.get('collection'): filters.append(RadcorActivity.collection_id == args['collection'])
        if args.get('type'): filters.append(RadcorActivity.activity_type.contains(args['type']))

        result = db.session.query(Task.status, func.count('*'))\
            .join(RadcorActivityHistory, RadcorActivityHistory.task_id==Task.id)\
            .join(RadcorActivity, RadcorActivity.id==RadcorActivityHistory.activity_id)\
            .filter(*filters)\
            .group_by(Task.status)\
            .all()

        return {r[0]: r[1] for r in result}

    @classmethod
    def count_activities_with_date(cls, args: dict):
        """Count activities by date."""
        filters = []
        if args.get('start_date'): filters.append(RadcorActivityHistory.start >= '{}T00:00'.format(args['start_date']))
        if args.get('last_date'): filters.append(RadcorActivityHistory.start <= '{}T23:59'.format(args['last_date']))
        if args.get('collection'): filters.append(RadcorActivity.collection_id == args['collection'])
        if args.get('type'): filters.append(RadcorActivity.activity_type.contains(args['type']))

        result = db.session.query(RadcorActivityHistory.start.cast(Date), Task.status, func.count('*'))\
            .join(RadcorActivityHistory, RadcorActivityHistory.task_id==Task.id)\
            .join(RadcorActivity, RadcorActivity.id==RadcorActivityHistory.activity_id)\
            .filter(*filters)\
            .group_by(RadcorActivityHistory.start.cast(Date), Task.status)\
            .order_by(RadcorActivityHistory.start.cast(Date))\
            .all()

        return [{'date': r[0].strftime('%Y-%m-%d'), 'status': r[1], 'count': r[2]} for r in result]

    @classmethod
    def get_collections_activities(cls):
        """Retrieve activities distinct."""
        activities = RadcorActivity.query().distinct(RadcorActivity.collection_id).all()
        return [act.collection_id for act in activities]

    @classmethod
    def get_unsuccessfully_activities(cls):
        """Retrieve all failed activities."""
        result = db.engine.execute("\
            WITH activity_tasks AS (\
                SELECT a.sceneid AS sceneid,\
                    max(h.start) AS start_date\
                FROM activity_history h, celery_taskmeta t, activities a\
                WHERE h.task_id = t.id AND a.id = h.activity_id\
                GROUP BY 1\
            ), failed_tasks AS (\
                SELECT a.id AS id,\
                a.sceneid AS sceneid,\
                a.activity_type AS type\
                FROM activity_tasks act, activity_history h, celery_taskmeta t, activities a\
                WHERE a.sceneid = act.sceneid AND h.start = act.start_date AND a.id = h.activity_id \
                    AND t.id = h.task_id AND t.status != 'SUCCESS' ORDER BY a.id\
            ) \
            SELECT count(*) FROM failed_tasks").first()
        return {"result": result.count}
