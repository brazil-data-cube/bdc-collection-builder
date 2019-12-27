# Python
from datetime import date as Date, datetime, timedelta
from typing import List, Optional
# 3rdparty
from celery import chord, group
from dateutil.relativedelta import relativedelta
from geoalchemy2 import func
from sqlalchemy import or_
from werkzeug.exceptions import BadRequest, NotAcceptable, NotFound

from bdc_db.models.base_sql import BaseModel
from bdc_db.models import Asset, Band, Collection, CollectionItem, db, Tile
from .forms import CollectionForm
from .tasks import warp, merge, blend


class CubeBusiness:
    @classmethod
    def create(cls, params: dict):
        # add WARPED type if not send
        if 'WARPED' not in [func.upper() for func in params['composite_function_list']]:
            params['composite_function_list'].append('WARPED')

        # generate cubes metadata
        cubes_db = Collection.query().filter().all()
        cubes = []
        cubes_serealized = []

        for composite_function in params['composite_function_list']:
            c_function_id = composite_function.upper()
            cube_id = '{}{}'.format(params['datacube'], c_function_id)

            raster_size_id = '{}-{}'.format(params['grs'], int(params['resolution']))

            # add cube
            if not list(filter(lambda x: x.id == cube_id, cubes)) and not list(filter(lambda x: x.id == cube_id, cubes_db)):
                cube = Collection(
                    id=cube_id,
                    temporal_composition_schema_id=params['temporal_schema'],
                    raster_size_schema_id=raster_size_id,
                    composite_function_schema_id=c_function_id,
                    grs_schema_id=params['grs'],
                    description=params['description'],
                    radiometric_processing=None,
                    geometry_processing=None,
                    sensor=None,
                    is_cube=True,
                    oauth_scope=None,
                    bands_quicklook=','.join(params['bands_quicklook'])
                )

                cubes.append(cube)
                cubes_serealized.append(CollectionForm().dump(cube))

        BaseModel.save_all(cubes)

        bands = []

        for cube in cubes:
            # save bands
            for band in params['bands']['names']:
                band = band.strip()
                bands.append(Band(
                    name=band,
                    collection_id=cube.id,
                    min=params['bands']['min'],
                    max=params['bands']['max'],
                    fill=params['bands']['fill'],
                    scale=params['bands']['scale'],
                    data_type=params['bands']['data_type'],
                    common_name=band,
                    resolution_x=params['resolution'],
                    resolution_y=params['resolution'],
                    resolution_unit='m',
                    description='',
                    mime_type='image/tiff'
                ))

        BaseModel.save_all(bands)

        return cubes_serealized, 201

    @staticmethod
    def _prepare_blend_dates(cube: Collection, warp_merge: dict, start_date: Date, end_date: Date):
        requestedperiods = {}

        t_composite_schema = cube.temporal_composition_schema

        # tdtimestep = datetime.timedelta(days=int(t_composite_schema.temporal_composite_t))
        # stepsperperiod = int(round(365./timestep))

        # start_date = datetime.strptime('%Y-%m-%d', start_date).date()

        if end_date is None:
            end_date = Date()

        if t_composite_schema.temporal_schema is None:
            periodkey = startdate + '_' + startdate + '_' + end_date
            requestedperiod = []
            requestedperiod.append(periodkey)
            requestedperiods[startdate] = requestedperiod
            return requestedperiods

        if t_composite_schema.temporal_schema == 'M':
            requestedperiod = []

            offset = relativedelta(months=int(t_composite_schema.temporal_composite_t))

            current_date = start_date
            current_date.replace(day=1)

            next_month_first = current_date + offset

            if end_date < current_date:
                print('Set end date to the end of month')
                end_date = current_date + offset

            while current_date < end_date:
                current_date_str = current_date.strftime('%Y-%m')

                requestedperiods.setdefault(current_date_str, dict())

                for item_date, scenes in warp_merge.items():
                    scene_date = datetime.strptime(item_date, '%Y-%m-%d').date()

                    if scene_date >= current_date:
                        requestedperiods[current_date_str][item_date] = scenes

                current_date += offset

            return requestedperiods

        return

    @classmethod
    def search_stac(cls, cube: Collection, collection_name: str, tiles: List[str], start_date: str, end_date: str):
        from stac import STAC

        stac_cli = STAC('http://brazildatacube.dpi.inpe.br/bdc-stac/0.7.0/')

        filter_opts = dict(
            time='{}/{}'.format(start_date, end_date),
            limit=100000
        )

        bbox_result = db.session.query(
            func.ST_AsText(func.ST_BoundingDiagonal(func.ST_Force2D(Tile.geom_wgs84)))
        ).filter(
            # Tile.grs_schema_id == cube.grs_schema_id,
            Tile.id.in_(tiles)
        ).all()

        result = dict()

        stac_collection = stac_cli.collection(collection_name)
        collection_bands = stac_collection['properties']['bdc:bands']

        for res in bbox_result:
            bbox = res[0][res[0].find('(') + 1:res[0].find(')')]
            bbox = bbox.replace(' ', ',')
            filter_opts['bbox'] = bbox
            items = stac_cli.collection_items(collection_name, filter=filter_opts)


            for feature in items['features']:
                feature_date_time_str = feature['properties']['datetime']
                feature_date = datetime.strptime(feature_date_time_str, '%Y-%m-%dT%H:%M:%S').date()
                feature_date_str = feature_date.strftime('%Y-%m-%d')

                result.setdefault(feature_date_str, dict())

                ## Comment/Uncomment these lines in order to retrieve data grouped by band/scene
                for band in collection_bands:
                    if feature['assets'].get(band):
                        result[feature_date_str].setdefault(band, dict())

                        asset_definition = dict(
                            url=feature['assets'][band]['href'],
                            band=collection_bands[band]
                        )

                        result[feature_date_str][band][feature['id']] = asset_definition
                ## end comment

                ## Uncomment these lines in order to retrieve data grouped by scene/band
                # result[feature_date_str].setdefault(feature['id'], dict())

                # for band in collection_bands:
                #     if feature['assets'].get(band):
                #         asset_definition = dict(
                #             url=feature['assets'][band]['href'],
                #             band=collection_bands[band]
                #         )

                #         result[feature_date_str][feature['id']][band] = asset_definition

        return result

    @staticmethod
    def create_activity(collection: str, scene: str, activity_type: str, scene_type: str, **parameters):
        return dict(
            collection_id=collection,
            activity_type=activity_type,
            tags=parameters.get('tags', []),
            sceneid=scene,
            scene_type=scene_type,
            args=parameters
        )

    @classmethod
    def process(cls,
                datacube: str,
                collections: List[str],
                tiles: Optional[List[str]]=None,
                start_date: Optional[str]=None,
                end_date: Optional[str]=None):
        cube = Collection.query().filter(Collection.id == datacube).first()

        if cube is None:
            raise NotFound('Cube {} not found'.format(datacube))

        if not cube.is_cube:
            raise NotAcceptable('{} is not a datacube'.format(datacube))

        for collection_name in collections:
            stac = CubeBusiness.search_stac(cube, collection_name, tiles, start_date, end_date)

            res = CubeBusiness._prepare_blend_dates(cube, stac, start_date, end_date)

            blend_tasks = []

            for blend_date, merges in res.items():
                merge_tasks = []

                for merge_date, bands in merges.items():
                    for band_name, scenes in bands.items():
                        warp_tasks = []

                        for scene, asset in scenes.items():
                            args = dict(datacube=cube.id, asset=asset)
                            activity = CubeBusiness.create_activity(cube.id, scene, 'WARP', 'WARPED', **args)
                            warp_tasks.append(warp.s(activity))

                        task = chord(warp_tasks, body=merge.s())
                        merge_tasks.append(task)

                task = chord(merge_tasks)(blend.s())
                blend_tasks.append(task)

            tasks = group(blend_tasks)
            tasks.apply_async()

            # for blend_date, merges in res.items():
            #     blend_at_start = datetime.strptime(blend_date, '%Y-%m-%d').date().replace(day=1)

            #     merge_tasks = []

            #     for scene, assets in merges.items():
            #         warp_tasks = []

            #         for asset in assets.values():
            #             args = dict(datacube=cube.id, asset=asset)
            #             activity = CubeBusiness.create_activity(cube.id, scene, 'WARP', 'WARPED', **args)
            #             warp_tasks.append(warp.s(cube.id, asset))

            #         merge_task = chord(warp_tasks)(merge.s())

            #         merge_tasks.append(merge_task)

            #     blend_task = chord(merge_tasks)(blend.s())
            #     blend_tasks.append(blend_task)

            tasks = group(blend_tasks)

            tasks.apply_async()

            return res

        # # Query Criterion based in arguments
        # criterion = [
        #     CollectionItem.collection_id == collection for collection in collections
        # ]

        # if tiles:
        #     criterion.append(or_(CollectionItem.tile_id == tile for tile in tiles))

        # if start_date:
        #     criterion.append(CollectionItem.item_date >= start_date)

        # if end_date:
        #     criterion.append(CollectionItem.item_date <= end_date)

        # items = CollectionItem.query().filter(
        #     *criterion
        # ).order_by(CollectionItem.item_date.asc())

        # warp_merge = dict()

        # for collection_item in items:
        #     item_date_str = collection_item.item_date.strftime('%Y-%m-%d')

        #     warp_merge.setdefault(item_date_str, dict())

        #     assets = Asset.query().filter(Asset.collection_item_id == collection_item.id).all()

        #     warp_merge[item_date_str][collection_item.id] = assets