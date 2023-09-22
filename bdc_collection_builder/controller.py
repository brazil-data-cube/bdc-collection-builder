#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2022 INPE.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/gpl-3.0.html>.
#

"""Define base interface for Celery Tasks."""

# Python Native
import json
from datetime import datetime, timedelta

# 3rdparty
from bdc_catalog.models import Collection, GridRefSys, Item, Provider, Tile
from celery import chain, group
from celery.backends.database import Task
from dateutil.relativedelta import relativedelta
from flask import current_app
from sqlalchemy import Date, and_, func, or_
from werkzeug.exceptions import BadRequest, abort

# Builder
from .celery.tasks import correction, download, post, publish
from .collections.collect import get_provider_order
from .collections.models import (ActivitySRC, RadcorActivity,
                                 RadcorActivityHistory, db)
from .collections.utils import get_or_create_model, get_provider, safe_request
from .forms import CollectionForm, RadcorActivityForm, SimpleActivityForm

from copy import deepcopy


def _generate_periods(start_date: datetime, end_date: datetime, unit='m'):
    periods = []

    def next_period(last):
        if unit == 'm':
            period = last + relativedelta(months=1)
            return datetime(period.year, period.month, 1)
        elif unit == 'y':
            period = last + relativedelta(years=1)
            return datetime(period.year, 1, 1)

    start_period = start_date
    end_period = start_date

    while end_period <= end_date:
        end_period = next_period(start_period) - timedelta(days=1)

        if end_period > end_date and start_period < end_date:
            periods.append([start_period, end_date])
        elif end_period <= end_date:
            periods.append([start_period, end_period])
            start_period = next_period(start_period)

    return periods


class RadcorBusiness:
    """Define an interface for handling entire module business."""

    @classmethod
    def restart(cls, ids=None, status=None, activity_type=None, sceneid=None, collection_id=None, action=None, **kwargs):
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
                raise BadRequest('Requires "collection_id" and "activity_type" to search by scene id.')

            scenes = sceneid.split(',') if isinstance(sceneid, str) else sceneid
            restrictions.append(RadcorActivity.sceneid.in_(scenes))

        if len(restrictions) == 0:
            raise BadRequest('Invalid restart. Requires "ids", "activity_type" or "status"')

        activities = db.session.query(RadcorActivity).filter(*restrictions).all()

        # Define a start wrapper in order to preview or start activity
        start_activity = cls.dispatch if str(action).lower() == 'start' else lambda _: _

        serializer = SimpleActivityForm()

        serialized_activities = []

        for activity in activities:
            start_activity(activity)
            serialized_activities.append(serializer.dump(activity))

        return serialized_activities

    @classmethod
    def create_activity(cls, activity, parent=None):
        """Persist an activity on database."""
        where = dict(
            sceneid=activity['sceneid'],
            activity_type=activity['activity_type'],
            collection_id=activity['collection_id']
        )

        model, created = get_or_create_model(RadcorActivity, defaults=activity, **where)

        if created:
            db.session.add(model)

        if parent:
            relation_defaults = dict(
                activity=model,
                parent=parent
            )

            _relation, _created = get_or_create_model(
                ActivitySRC,
                defaults=relation_defaults,
                **relation_defaults
            )

        return model, created

    @classmethod
    def dispatch(cls, activity: RadcorActivity, skip_collection_id=None):
        """Search by activity and dispatch the respective task.

        TODO: Support object to skip tasks using others values.

        Args:
            activity - Activity to dispatch
            skip_collection_id - Skip the tasks with has the given collection id.
        """
        def _dispatch_task(_activity, parent=None):
            dump = RadcorActivityForm().dump(_activity)

            _task = cls._task_definition(_activity.activity_type)

            if not _activity.children:
                if parent is None:
                    return _task.s(dump)
                return _task.s()

            tasks = []

            for child in _activity.children:
                if skip_collection_id == child.activity.collection_id:
                    continue

                tasks.append(_dispatch_task(child.activity, parent=_activity))

            handler = _task.s(dump) if parent is None else _task.s()

            return handler | chain(*tasks)

        task = _dispatch_task(activity, parent=None)
        task.apply_async()

    @classmethod
    def _task_definition(cls, task_type):
        """Get a task by string.

        TODO: We should consider to import dynamically using importlib or factory method.
        """
        if task_type == 'download':
            _task = download
        elif task_type == 'correction':
            _task = correction
        elif task_type == 'publish':
            _task = publish
        elif task_type == 'post':
            _task = post
        else:
            raise RuntimeError(f'Task {task_type} not supported.')

        return _task

    @classmethod
    def _activity_definition(cls, collection_id, activity_type, scene, **kwargs):
        data = {}
        include_meta = kwargs.get("include_meta", False)
        if include_meta:
            data["scene_meta"] = scene

        return dict(
            collection_id=collection_id,
            activity_type=activity_type,
            tags=kwargs.get('tags', []),
            sceneid=scene.scene_id,
            scene_type='SCENE',
            args=dict(
                cloud=scene.cloud_cover,
                **data,
                **kwargs
            )
        )

    @classmethod
    def radcor(cls, args: dict):
        """Search for Landsat/Sentinel Images and dispatch download task."""
        args.setdefault('cloud', 100)

        cloud = float(args['cloud'])
        action = args.get('action', 'preview')

        collections = Collection.query().filter(Collection.collection_type.in_(['collection', 'cube'])).all()

        # TODO: Review this code. The collection name is not unique anymore.
        collections_map = {f'{c.name}-{c.version}': c.id for c in collections}

        tasks = args.get('tasks', [])

        force = args.get('force', False)
        catalog_args = args.get('catalog_args', dict())
        options = dict()
        options.update(args.get("catalog_search_args", {}))

        if 'platform' in args:
            options['platform'] = args['platform']

        if args.get("geom"):
            options["geom"] = args["geom"]
        elif 'scenes' not in args and 'tiles' not in args:
            # Deprecated
            w, e = float(args['w']), float(args['e'])
            s, n = float(args['s']), float(args['n'])
            bbox = [w, s, e, n]
            options['bbox'] = bbox

        try:
            catalog_provider, provider = get_provider(catalog=args['catalog'], **catalog_args)

            with safe_request():
                if 'scenes' in args:
                    result = []

                    unique_scenes = set(args['scenes'])

                    for scene in unique_scenes:
                        query_result = provider.search(
                            query=args['dataset'],
                            filename=f'{scene}*',
                            **options
                        )

                        result.extend(query_result)
                elif 'tiles' in args:
                    result = []
                    for tile in args['tiles']:
                        query_result = provider.search(
                            query=args['dataset'],
                            tile=tile,
                            start_date=args['start'],
                            end_date=args['end'],
                            cloud_cover=cloud,
                            **options
                        )
                        result.extend(query_result)
                else:
                    result = provider.search(
                        query=args['dataset'],
                        start_date=args['start'],
                        end_date=args['end'],
                        cloud_cover=cloud,
                        **options
                    )

            tasks_collections = _get_tasks_collections(tasks)
            # Preload items that have been published
            items_cache = (
                db.session.query(Item.name, Item.collection_id)
                .filter(
                    Item.collection_id.in_([c.id for c in tasks_collections]),
                    Item.start_date >= args['start'],
                    Item.end_date <= args['end'],
                    Item.name.in_([scene.scene_id for scene in result])
                )
                .all()
            )
            items_map = {}
            for item in items_cache:
                items_map.setdefault(item.name, [])
                items_map[item.name].append(item.collection_id)

            def _recursive(scene, task, parent=None, parallel=True, pass_args=True):
                """Create task dispatcher recursive."""
                collection_id = collections_map[task['collection']]
                # Create activity definition example
                activity = cls._activity_definition(collection_id, task['type'], scene, **task['args'])
                activity['args'].update(dict(catalog=args['catalog'], dataset=args['dataset'], catalog_args=catalog_args))

                _task = cls._task_definition(task['type'])
                # Try to create activity in database and the parent if there is.
                instance, created = cls.create_activity(activity, parent)

                instance.args = deepcopy(activity['args'])
                instance.save(commit=False)

                if activity["sceneid"] in items_map:
                    cached_collections = items_map[activity["sceneid"]]
                    # Skip all scenes that were already published
                    if collection_id in cached_collections and not force:
                        return None

                dump = RadcorActivityForm().dump(instance)
                dump['args'].update(activity['args'])

                keywords = dict(collection_id=collection_id, activity_type=task['type'])
                # If no children
                if not task.get('tasks'):
                    if parent is None:
                        return _task.s(dump, force=force)
                    return _task.s(**keywords)

                res = []

                for child in task['tasks']:
                    # When triggering children, use parallel=False to use chain workflow
                    child_task = _recursive(scene, child, parent=instance, parallel=False, pass_args=False)

                    if child_task:
                        res.append(child_task)

                handler = group(*res) if parallel else chain(*res)

                arguments = []

                if pass_args:
                    arguments.append(dump)

                return _task.s(*arguments, **keywords) | handler

            if action == 'start':
                to_dispatch = []

                with db.session.begin_nested():
                    for task in tasks:
                        if task['type'] == 'download':
                            cls.validate_provider(collections_map[task['collection']])

                        for scene_result in result:
                            children_task = _recursive(scene_result, task, parent=None)

                            if children_task:
                                to_dispatch.append(children_task)

                db.session.commit()

                if len(to_dispatch) > 0:
                    group(to_dispatch).apply_async()
        except Exception:
            db.session.rollback()
            raise

        return result

    @classmethod
    def validate_provider(cls, collection_id):
        """Check if the given collection has any provider set."""
        collection = Collection.query().filter(Collection.id == collection_id).first_or_404()

        download_order = get_provider_order(collection, lazy=True)

        if len(download_order) == 0:
            abort(400, f'Collection {collection.name} does not have any data provider set.')

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
        if args.get('start_date'):
            filters.append(RadcorActivityHistory.start >= '{}T00:00'.format(args['start_date']))
        if args.get('last_date'):
            filters.append(RadcorActivityHistory.start <= '{}T23:59'.format(args['last_date']))
        if args.get('collection'):
            filters.append(RadcorActivity.collection_id == args['collection'])
        if args.get('type'):
            filters.append(RadcorActivity.activity_type.contains(args['type']))

        result = db.session.query(Task.status, func.count('*'))\
            .join(RadcorActivityHistory, RadcorActivityHistory.task_id == Task.id)\
            .join(RadcorActivity, RadcorActivity.id == RadcorActivityHistory.activity_id)\
            .filter(*filters)\
            .group_by(Task.status)\
            .all()

        return {r[0]: r[1] for r in result}

    @classmethod
    def count_activities_with_date(cls, args: dict):
        """Count activities by date."""
        filters = []
        if args.get('start_date'):
            filters.append(RadcorActivityHistory.start >= '{}T00:00'.format(args['start_date']))
        if args.get('last_date'):
            filters.append(RadcorActivityHistory.start <= '{}T23:59'.format(args['last_date']))
        if args.get('collection'):
            filters.append(RadcorActivity.collection_id == args['collection'])
        if args.get('type'):
            filters.append(RadcorActivity.activity_type.contains(args['type']))

        result = db.session.query(RadcorActivityHistory.start.cast(Date), Task.status, func.count('*'))\
            .join(RadcorActivityHistory, RadcorActivityHistory.task_id == Task.id)\
            .join(RadcorActivity, RadcorActivity.id == RadcorActivityHistory.activity_id)\
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

    @classmethod
    def check_scenes(cls, collections: str, start_date: datetime, end_date: datetime,
                     catalog: str = None, dataset: str = None,
                     grid: str = None, tiles: list = None, bbox: list = None, catalog_kwargs=None, only_tiles=False):
        """Check for the scenes in remote provider and compares with the Collection Builder."""
        bbox_list = []
        if grid and tiles:
            grid = GridRefSys.query().filter(GridRefSys.name == grid).first_or_404(f'Grid "{grid}" not found.')
            geom_table = grid.geom_table

            rows = db.session.query(
                geom_table.c.tile,
                func.ST_Xmin(func.ST_Transform(geom_table.c.geom, 4326)).label('xmin'),
                func.ST_Ymin(func.ST_Transform(geom_table.c.geom, 4326)).label('ymin'),
                func.ST_Xmax(func.ST_Transform(geom_table.c.geom, 4326)).label('xmax'),
                func.ST_Ymax(func.ST_Transform(geom_table.c.geom, 4326)).label('ymax'),
            ).filter(geom_table.c.tile.in_(tiles)).all()
            for row in rows:
                bbox_list.append((row.tile, (row.xmin, row.ymin, row.xmax, row.ymax)))
        else:
            bbox_list.append(('', bbox))

        instance, provider = get_provider(catalog)

        collection_map = dict()
        collection_ids = list()

        for _collection in collections:
            collection, version = _collection.split('-')

            collection = Collection.query().filter(
                Collection.name == collection,
                Collection.version == version
            ).first_or_404(f'Collection "{collection}-{version}" not found.')

            collection_ids.append(collection.id)
            collection_map[_collection] = collection

        options = dict(start_date=start_date, end_date=end_date)
        if catalog_kwargs:
            options.update(catalog_kwargs)

        redis = current_app.redis
        output = dict(
            collections={cname: dict(total_scenes=0, total_missing=0, missing_external=[]) for cname in collections}
        )

        items = {cid: set() for cid in collection_ids}
        external_scenes = set()

        for tile, _bbox in bbox_list:
            with redis.pipeline() as pipe:
                if only_tiles:
                    entry = tile

                    if catalog == 'MODIS':
                        tile = f'h{tile[1:3]}v{tile[-2:]}'

                    options['tile'] = tile
                else:
                    options['bbox'] = _bbox
                    entry = _bbox

                periods = _generate_periods(start_date.replace(tzinfo=None), end_date.replace(tzinfo=None))

                for period_start, period_end in periods:
                    _items = db.session.query(Item.name, Item.collection_id).filter(
                        Item.collection_id.in_(collection_ids),
                        func.ST_Intersects(
                            func.ST_MakeEnvelope(
                                *_bbox, func.ST_SRID(Item.geom)
                            ),
                            Item.geom
                        ),
                        or_(
                            and_(Item.start_date >= period_start, Item.start_date <= period_end),
                            and_(Item.end_date >= period_start, Item.end_date <= period_end),
                            and_(Item.start_date < period_start, Item.end_date > period_end),
                        )
                    ).order_by(Item.name).all()

                    for item in _items:
                        items[item.collection_id].add(item.name)

                    options['start_date'] = period_start.strftime('%Y-%m-%d')
                    options['end_date'] = period_end.strftime('%Y-%m-%d')

                    key = f'scenes:{catalog}:{dataset}:{period_start.strftime("%Y%m%d")}_{period_end.strftime("%Y%m%d")}_{entry}'

                    pipe.get(key)
                    provider_scenes = []

                    if not redis.exists(key):
                        provider_scenes = provider.search(dataset, **options)
                        provider_scenes = [s.scene_id for s in provider_scenes]

                        pipe.set(key, json.dumps(provider_scenes))

                    external_scenes = external_scenes.union(set(provider_scenes))

                cached_scenes = pipe.execute()

                for cache in cached_scenes:
                    # When cache is True, represents set the value were cached.
                    if cache is not None and cache is not True:
                        external_scenes = external_scenes.union(set(json.loads(cache)))

        output['total_external'] = len(external_scenes)
        for _collection_name, _collection in collection_map.items():
            _items = set(items[_collection.id])
            diff = list(external_scenes.difference(_items))

            output['collections'][_collection_name]['total_scenes'] = len(_items)
            output['collections'][_collection_name]['total_missing'] = len(diff)
            output['collections'][_collection_name]['scenes'] = list(external_scenes)
            output['collections'][_collection_name]['missing_external'] = diff

            for cname, _internal_collection in collection_map.items():
                if cname != _collection_name:
                    diff = list(_items.difference(set(items[_internal_collection.id])))
                    output['collections'][_collection_name][f'total_missing_{cname}'] = len(diff)
                    output['collections'][_collection_name][f'missing_{cname}'] = diff

        return output

    @classmethod
    def list_collections(cls):
        """List the available collections in database."""
        collections = Collection.query()\
            .filter(Collection.collection_type.in_(['collection', 'cube']))\
            .order_by(Collection.id)\
            .all()

        return CollectionForm().dump(collections, many=True)

    @classmethod
    def list_grids(cls, grid_id: int = None, bbox=None):
        """List all available grids in database."""
        if grid_id:
            grids = [GridRefSys.query().filter(GridRefSys.id == grid_id).first_or_404('Grid not found.')]
        else:
            grids = GridRefSys.query().order_by(GridRefSys.name).all()

        output = []

        for grid in grids:
            g = dict(id=grid.id, name=grid.name, description=grid.description)

            geom_table = grid.geom_table

            if grid_id:
                where = []

                if bbox:
                    where.append(
                        func.ST_Intersects(
                            func.ST_Transform(geom_table.c.geom, 4326),
                            func.ST_MakeEnvelope(*bbox, 4326)
                        )
                    )

                rows = db.session.query(
                    geom_table.c.tile,
                    func.ST_AsGeoJSON(func.ST_Transform(geom_table.c.geom, 4326)).label('geom')
                ).filter(*where).all()

                g['geom'] = dict(
                    type='FeatureCollection',
                    features=[
                        dict(
                            type='Feature',
                            geometry=json.loads(row[1]),
                            properties=dict(
                                tile=row[0]
                            )
                        )
                        for row in rows
                    ]
                )
            output.append(g)

        return output[0] if len(output) == 1 else output

    @classmethod
    def list_collection_tiles(cls, collection_id: int):
        """List the tiles related with collection items."""
        tiles = db.session\
            .query(Tile.name)\
            .join(Item, Tile.id == Item.tile_id)\
            .filter(Item.collection_id == collection_id)\
            .distinct(Tile.name) \
            .all()

        return [t.name for t in tiles]

    @classmethod
    def list_catalogs(cls):
        """List the supported providers."""
        providers = Provider.query().order_by(Provider.id)

        return [dict(id=p.id, name=p.name) for p in providers]


def _get_tasks_collections(tasks):
    """Retrieve the collections associated in tasks for processing."""
    out = []

    def _get_task_collections(task):
        collections = [task["collection"]]

        for child in task.get("tasks", []):
            cs = _get_task_collections(child)
            if cs:
                collections.extend(cs)

        return collections

    if len(tasks) == 0:
        return out

    for task in tasks:
        collections_ = _get_task_collections(task)
        out.extend(collections_)

    rows = Collection.query().filter(Collection.identifier.in_(out)).all()
    return rows
