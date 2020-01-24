# Python Native
from os import path as resource_path
import glob
import logging

# 3rdparty
from sqlalchemy import or_
from werkzeug.exceptions import BadRequest

# BDC Scripts
from bdc_db.models import db, Collection, CollectionTile
from bdc_scripts.config import Config
from bdc_scripts.db import db_aws
from bdc_scripts.radcor.forms import RadcorActivityForm
from bdc_scripts.radcor.models import RadcorActivity, RadcorActivityHistory
from bdc_scripts.radcor.utils import dispatch, get_landsat_scenes, get_sentinel_scenes, get_or_create_model

# Consts
CLOUD_DEFAULT = 90
DESTINATION_DIR = Config.DATA_DIR


class RadcorBusiness:
    @classmethod
    def start(cls, activity):
        """Dispatch the celery tasks"""

        return dispatch(activity)

    @classmethod
    def restart(cls, ids=None, status=None, activity_type=None):
        restrictions = []

        if ids:
            restrictions.append(or_(RadcorActivity.id == id for id in ids))

        if status:
            restrictions.append(RadcorActivityHistory.task.has(status=status))

        if activity_type:
            restrictions.append(RadcorActivity.activity_type == activity_type)

        if len(restrictions) == 0:
            raise BadRequest('Invalid restart. You must provide query restriction such "ids", "activity_type" or "status"')

        activities = db.session.query(RadcorActivity).filter(*restrictions).all()

        for activity in activities:
            dumps = RadcorActivityForm().dump(activity)

            cls.start(dumps)

        return activities

    @classmethod
    def create_tile(cls, grs, tile, collection, engine=db):
        with engine.session.begin_nested():
            restriction = dict(
                grs_schema_id=grs,
                tile_id=tile,
                collection_id=collection
            )

            _, _ = get_or_create_model(CollectionTile, defaults=restriction, engine=engine, **restriction)

        engine.session.commit()

    @classmethod
    def radcor(cls, args: dict):
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
                LC8SRfull = resource_path.join(base_dir, '{}/{}/'.format(yyyymm,tileid))
                template =  LC8SRfull+'{}.png'.format(sceneid)
                LC8SRfiles = glob.glob(template)
                if len(LC8SRfiles) > 0:
                    scene['status'] = 'DONE'
                    continue
                scene['status'] = 'NOTDONE'

                tile = '{}{}'.format(scene['path'], scene['row'])
                RadcorBusiness.create_tile('WRS2', tile, 'LC8DN', engine=db)
                RadcorBusiness.create_tile('WRS2', tile, 'LC8SR', engine=db)
                RadcorBusiness.create_tile('WRS2', tile, 'LC8SR', engine=db_aws)

                activity = dict(
                    collection_id='LC8DN',
                    activity_type='downloadLC8',
                    tags=args.get('tags', '').split(','),
                    sceneid=sceneid,
                    scene_type='SCENE',
                    args=dict(
                        link=scene['link'],
                        file=base_dir,
                        satellite='LC8',
                        cloud=scene.get('cloud')
                    )
                )

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

                scene['status'] = 'NOTDONE'

                activities = RadcorActivity.is_started_or_done(sceneid=scene['sceneid'])

                if len(activities) > 0:
                    logging.warning('radcor - activity already done {}'.format(len(activities)))
                    continue

                RadcorBusiness.create_tile('MGRS', scene['tileid'], 'S2TOA', engine=db)
                RadcorBusiness.create_tile('MGRS', scene['tileid'], 'S2SR_SEN28', engine=db)
                RadcorBusiness.create_tile('MGRS', scene['tileid'], 'S2SR_SEN28', engine=db_aws)

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
                        cloud=scene.get('cloud')
                    )
                )

                scenes[id] = scene

                if action == 'start':
                    cls.start(activity)

        return scenes
