import datetime
import os
import time
from celery import Celery
from rc_maestro.sentinel import publish as publish_S2, \
                                download as download_S2, \
                                upload as upload_S2
from rc_maestro.utils import do_upsert, do_update


celery = Celery(__name__,
                backend='rpc://',
                broker='pyamqp://guest@localhost')


def upsert_activity(activity, fields=None):
    if fields is None:
        fields = ['id', 'status', 'link', 'file', 'start', 'end', 'elapsed', 'retcode', 'message']

    do_upsert('activities', activity, fields)


@celery.task
def download_sentinel(activity):
    print('Download sentinel... COGS')

    activity['retcode'] = 0
    activity.update(elapsed=None)
    file = download_S2(activity)

    activity['file'] = file

    if file is None:
        activity['file'] = ''
        retcode = 1
        activity['status'] = 'ERROR'
        activity['message'] = 'Abormal Execution'

    is_level_2a = activity['file'].find('MSIL2A') != -1
    safe_l2a_full = activity['file'].replace('MSIL1C', 'MSIL2A')

    do_update('activities', activity)

    new_activity = {
        'id': None,
        'priority': 2,
        'app': 'publishS2',
        'status': 'NOTDONE',
        'message': '',
        'retcode': 0,
    }

    if not os.path.exists(safe_l2a_full) and not is_level_2a:
        new_activity.update(app='sen2cor')
    else:
        new_activity.update(priority=0, app='publishS2')

    upsert_activity(new_activity)

    return new_activity


@celery.task
def publish_sentinel(activity):
    print('Publishing sentinel... COGS')

    step_start = time.time()
    activity['start'] = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(step_start)))
    activity['status'] = 'DONE'
    activity['message'] = 'Normal Execution'
    retcode = publish_S2(activity)
    if retcode != 0:
        activity['file'] = ''
        activity['status'] = 'ERROR'
        activity['message'] = 'Abormal Execution'
    activity['retcode'] = retcode
    step_end = time.time()
    elapsedtime = step_end - step_start
    activity['end'] = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(step_end)))
    activity['elapsed'] = str(datetime.timedelta(seconds=elapsedtime))
    upsert_activity(activity)

    new_activity = {
        'id': None,
        'priority': 3,
        'app': 'uploadS2',
        'status': 'NOTDONE',
        'message': '',
        'retcode': 0,
    }

    upsert_activity(new_activity)

    return new_activity


@celery.task
def upload_sentinel(activity):
    print('Upload sentinel to AWS')

    exit_code = upload_S2(activity)


@celery.task
def publish_landsat(activity):
    print('Publishing landsat8... COGS')

    # upload_sentinel.delay(activity)


@celery.task
def upload_landsat(activity):
    print('Upload landsat8 to AWS')


@celery.task
def download_landsat(activity):
    print('Download sentinel... COGS')

    # upload_sentinel.delay(activity)