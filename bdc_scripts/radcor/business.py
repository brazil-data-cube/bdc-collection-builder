# Python Native
from os import path as resource_path
import glob
import logging

# BDC Scripts
from bdc_scripts.config import Config
from bdc_scripts.radcor.forms import RadcorActivityForm
from bdc_scripts.radcor.models import RadcorActivity
from bdc_scripts.radcor.utils import dispatch, get_landsat_scenes, get_sentinel_scenes

# Consts
CLOUD_DEFAULT = 90
DESTINATION_DIR = Config.DATA_DIR


class RadcorBusiness:
    @classmethod
    def start(cls, activity):
        """Dispatch the celery tasks"""

        return dispatch(activity)

    @classmethod
    def restart(cls, id):
        activity = RadcorActivity.get(id=id)
        # Skip pending or started tasks
        if activity.task.status in ['PENDING', 'STARTED']:
            logging.warning('Skipped activity {} - {}'.format(activity, activity.task.status))
        else:
            dumps = RadcorActivityForm().dump(activity)

            del dumps['task']

            cls.start(dumps)

        return activity

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
                base_dir = resource_path.join(DESTINATION_DIR, 'Repository/Archive/LC8SR')
                LC8SRfull = resource_path.join(base_dir, '{}/{}/'.format(yyyymm,tileid))
                template =  LC8SRfull+'{}.png'.format(sceneid)
                LC8SRfiles = glob.glob(template)
                if len(LC8SRfiles) > 0:
                    scene['status'] = 'DONE'
                    continue
                scene['status'] = 'NOTDONE'
                activity = {}
                activity['app'] = 'downloadLC8'
                activity['status'] = 'NOTDONE'
                activity['sceneid'] = scene['sceneid']
                activity['satellite'] = 'LC8'
                activity['priority'] = 1
                activity['link'] = scene['link']
                activity['file'] = base_dir
                cls.start(activity)
        if 'S2' in sat or 'S2SR' in sat:
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
                # Check if an equivalent sceneid has already been downloaded
                date = cc[2]
                tile = cc[5]
                files = glob.glob(productdir+'S*MSIL2A_{}*{}*.SAFE'.format(date,tile))
                if len(files) > 0:
                    logging.warning('radcor - {} already done'.format(sceneid))
                    scene['status'] = 'DONE'
                    continue

                safeL2Afull = productdir+sceneid.replace('MSIL1C','MSIL2A')+'.SAFE'
                if resource_path.exists(safeL2Afull):
                    logging.warning('radcor - scene exists {}'.format(safeL2Afull))
                    scene['status'] = 'DONE'
                    continue

                scene['status'] = 'NOTDONE'

                activities = RadcorActivity.is_started_or_done(sceneid=scene['sceneid'])

                if len(activities) > 0:
                    logging.warning('radcor - activity already done {}'.format(len(activities)))
                    continue

                activity = {}
                activity['file'] = base_dir
                activity['app'] = 'downloadS2'
                activity['sceneid'] = sceneid
                activity['satellite'] = 'S2'
                activity['priority'] = 1
                activity['link'] = scene['link']
                activity['status'] = 'NOTDONE'

                # logging.warning('radcor - activity new {}'.format(activity))
                scenes[id] = scene

                # activity.save()

                cls.start(activity)

        return scenes
