from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from celery import chain
from rc_maestro.celery import publish_sentinel, upload_sentinel, download_sentinel, \
                              publish_landsat, upload_landsat, download_landsat
# from rc_maestro.rc_maestro import setActivities
from rc_maestro.utils import do_command, do_query


app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})


def start():
    sql = "SELECT * FROM activities WHERE status = 'NOTDONE' ORDER BY priority,id"
    result = do_query(sql)

    for activity in result:
        activity_ctx = activity['app']
        if activity.get('start'):
            activity['start'] = activity['start'].strftime("%Y-%m-%d %H:%M:%S")

        if activity.get('end'):
            activity['end'] = activity['end'].strftime("%Y-%m-%d %H:%M:%S")

        activity['elapsed'] = str(activity['elapsed'] or '')

        tasks = []

        if activity_ctx == 'downloadS2':
            # Dispatch celery for downloadS2
            tasks.extend([download_sentinel.s(activity), publish_sentinel.s(), upload_sentinel.s()])
        elif activity['app'] == 'publishS2':
            tasks.extend([publish_sentinel.s(activity), upload_sentinel.s()])
        elif activity['app'] == 'uploadS2':
            tasks.extend([upload_sentinel.s(activity)])
        elif activity['app'] == 'downloadLC8':
            tasks.extend([download_landsat.s(activity), publish_landsat.s(), upload_landsat.s()])
        elif activity['app'] == 'publishLC8':
            tasks.extend([publish_landsat.s(activity), upload_landsat.s()])
        else:
            # Invalid Activity App
            pass

        task_chain = chain(*tasks)

        task_chain.apply_async()

        print('Activity {}: tasks "{}" scheduled.'.format(
            activity['id'],
            ",".join([signature.name.split('.')[-1] for signature in tasks])
        ))



@app.route('/restart', methods=['GET'])
def restart():
    # task = publish_sentinel.delay(dict(status="NOTDONE"))
    #
    # task.ready()
    msg = 'Rc_Maestro restarting:\n'
    id = request.args.get('id', None)
    if id is None:
        sql = '''
            UPDATE activities
               SET status='NOTDONE'
             WHERE (status = 'ERROR' OR status = 'DOING' OR status = 'SUSPEND')
        '''
    else:
        sql = "UPDATE activities SET status='NOTDONE' WHERE id = {}".format(id)

    do_command(sql)
    msg += 'sql - {}\n'.format(sql)
    # setActivities()
    # msg += 'ACTIVITIES - {}\n'.format(ACTIVITIES)

    start()
    return "Hello"
