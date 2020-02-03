import os,sys
from flask import Flask, request, make_response, render_template, abort, jsonify
import time
import datetime
from datetime import timedelta
import subprocess
import logging

app = Flask(__name__)
app.config['PROPAGATE_EXCEPTIONS'] = True
app.logger_name = "sen2cor"
handler = logging.FileHandler('sen2cor.log')
handler.setFormatter(logging.Formatter(
    '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
))

app.logger.addHandler(handler)

@app.route('/sen2cor')
def sen2cor():
	activity = {}
	activity['app'] = 'sen2cor'
	activity['id'] = request.args.get('id', None)
	activity['sceneid'] = request.args.get('sceneid', None)
	safeL1Cfull = activity['file'] = request.args.get('file', 'xxx')
	safeL1Cfull = '/dados' + activity['file']
	activity['start'] = request.args.get('start', None)
	step_start = time.time()
	cmd = 'L2A_Process --resolution 10 {0}'.format(safeL1Cfull)
	retcode = 1
	activity['status'] = 'ERROR'
	if os.path.exists(safeL1Cfull):
		app.logger.warning('sen2cor - safeL1Cfull {}'.format(safeL1Cfull))
	"""
	if not os.path.exists(safeL1Cfull):
		app.logger.warning('sen2cor - coco {}'.format(cmd))
		activity['status'] = 'ERROR'
		activity['message'] = 'No such file {}'.format(safeL1Cfull)
		activity['retcode'] = retcode
		step_end = time.time()
		elapsedtime = step_end - step_start
		activity['end'] = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(step_end)))
		activity['elapsed'] = str(datetime.timedelta(seconds=elapsedtime))
		app.logger.warning('sen2cor - return {}'.format(activity))
    	return jsonify(activity)
	"""
	app.logger.warning('sen2cor - calling {}'.format(cmd))
	retcode = subprocess.call(cmd, shell = True)
	app.logger.warning('sen2cor - retcode {}'.format(retcode))
	step_end = time.time()
	elapsedtime = step_end - step_start
	activity['end'] = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(step_end)))
	activity['elapsed'] = str(datetime.timedelta(seconds=elapsedtime))
	if retcode == 0:
		activity['status'] = 'DONE'
	else:
		activity['status'] = 'ERROR'
	activity['retcode'] = retcode
	app.logger.warning('sen2cor - return {}'.format(activity))
	return jsonify(activity)

if __name__ == "__main__":
	app.run(host="0.0.0.0", port=5031, debug=True)
