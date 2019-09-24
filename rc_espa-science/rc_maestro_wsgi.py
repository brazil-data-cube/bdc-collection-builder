import os,sys
from flask import Flask, request, make_response, render_template, abort, jsonify
import time
import datetime
from datetime import timedelta
import subprocess
import logging
import tarfile
import glob

app = Flask(__name__)
app.config['PROPAGATE_EXCEPTIONS'] = True
app.logger_name = "espa"
handler = logging.FileHandler('espa.log')
handler.setFormatter(logging.Formatter(
    '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
))

app.logger.addHandler(handler)
"""
usage: cli.py [-h] [--version] --order-id TEXT --input-product-id TEXT
              --product-type {landsat,modis,plot} --input-url TEXT
              [--espa-api TEXT] [--output-format {envi,gtiff,hdf-eos2,netcdf}]
              [--work-dir TEXT] [--dist-method TEXT] [--dist-dir TEXT]
              [--bridge-mode] [--include-cfmask] [--include-pixel-qa]
              [--include-customized-source-data]
              [--include-land-surface-temperature]
              [--include-surface-reflectance] [--include-sr-evi]
              [--include-sr-msavi] [--include-sr-nbr] [--include-sr-nbr2]
              [--include-sr-ndmi] [--include-sr-ndvi] [--include-sr-savi]
              [--include-top-of-atmosphere] [--include-brightness-temperature]
              [--include-surface-water-extent] [--include-statistics]
              [--resample-method {near,bilinear,cubic,cubicspline,lanczos}]
              [--pixel-size FLOAT] [--pixel-size-units {meters,dd}]
              [--extent-units {meters,dd}] [--extent-minx FLOAT]
              [--extent-maxx FLOAT] [--extent-miny FLOAT]
              [--extent-maxy FLOAT]
              [--target-projection {sinu,aea,utm,ps,lonlat}]
              [--false-easting FLOAT] [--false-northing FLOAT]
              [--datum {wgs84,nad27,nad83}] [--utm-north-south {north,south}]
              [--utm-zone INT] [--central-meridian FLOAT]
              [--latitude-true-scale FLOAT] [--longitude-pole FLOAT]
              [--origin-latitude FLOAT] [--std-parallel-1 FLOAT]
              [--std-parallel-2 FLOAT] [--dev-mode] [--dev-intermediate]
              [--debug]

ESPA Processing Command Line Interface

optional arguments:
  -h, --help            show this help message and exit
  --version             show program's version number and exit

order specifics:
  --order-id TEXT       Order ID
  --input-product-id TEXT
                        Input Product ID
  --product-type {landsat,modis,plot}
                        Type of product we are producing
  --input-url TEXT      Complete URL path to the input product. Supported
                        ("file://...", "http://...")
  --espa-api TEXT       URL for the ESPA API
  --output-format {envi,gtiff,hdf-eos2,netcdf}
                        Output format for the product
  --work-dir TEXT       Base processing directory
  --dist-method TEXT    Distribution method
  --dist-dir TEXT       Distribution directory
  --bridge-mode         Specify bridge processing mode

products:
  --include-cfmask      Include CFMask Products
  --include-pixel-qa    Include PixelQA Products
  --include-customized-source-data
                        Include Customized Source Data
  --include-land-surface-temperature
                        Include Land Surface Temperature
  --include-surface-reflectance
                        Include Surface Reflectance
  --include-sr-evi      Include Surface Reflectance based EVI
  --include-sr-msavi    Include Surface Reflectance based MSAVI
  --include-sr-nbr      Include Surface Reflectance based NBR
  --include-sr-nbr2     Include Surface Reflectance based NBR2
  --include-sr-ndmi     Include Surface Reflectance based NDMI
  --include-sr-ndvi     Include Surface Reflectance based NDVI
  --include-sr-savi     Include Surface Reflectance based SAVI
  --include-top-of-atmosphere
                        Include Top-of-Atmosphere Reflectance
  --include-brightness-temperature
                        Include Thermal Brightness Temperature
  --include-surface-water-extent
                        Include Surface Water Extent
  --include-statistics  Include Statistics

customization:
  --resample-method {near,bilinear,cubic,cubicspline,lanczos}
                        Resampling method to use
  --pixel-size FLOAT    Pixel size for the output product
  --pixel-size-units {meters,dd}
                        Units for the pixel size
  --extent-units {meters,dd}
                        Units for the extent
  --extent-minx FLOAT   Minimum X direction extent value
  --extent-maxx FLOAT   Maximum X direction extent value
  --extent-miny FLOAT   Minimum Y direction extent value
  --extent-maxy FLOAT   Maximum Y direction extent value
  --target-projection {sinu,aea,utm,ps,lonlat}
                        Reproject to this projection
  --false-easting FLOAT
                        False Easting reprojection value
  --false-northing FLOAT
                        False Northing reprojection value
  --datum {wgs84,nad27,nad83}
                        Datum to use during reprojection
  --utm-north-south {north,south}
                        UTM North or South
  --utm-zone INT        UTM Zone reprojection value
  --central-meridian FLOAT
                        Central Meridian reprojection value
  --latitude-true-scale FLOAT
                        Latitude True Scale reprojection value
  --longitude-pole FLOAT
                        Longitude Pole reprojection value
  --origin-latitude FLOAT
                        Origin Latitude reprojection value
  --std-parallel-1 FLOAT
                        Standard Parallel 1 reprojection value
  --std-parallel-2 FLOAT
                        Standard Parallel 2 reprojection value

developer:
  --dev-mode            Specify developer mode
  --dev-intermediate    Specify keeping intermediate data files
  --debug               Specify debug logging
"""

@app.route('/updatelads')
def updatelads():
	options = {}
	options['start_year'] = None
	options['end_year'] = None
	options['today'] = None
	options['quarterly'] = None
	for key in request.args:
		options[key] = request.args.get(key)

	syear = options['start_year']		# starting year
	eyear = options['end_year']			# ending year
	today = options['today']			# process most recent year of data
	quarterly = options['quarterly']	# process today back to START_YEAR

# check the arguments
	if (today == None) and (quarterly == None) and \
		(syear == None or eyear == None):
		msg = ('Invalid command line argument combination.\n')
		app.logger.error(msg)
		return msg

	params = ''
	for key,val in options.items():
		if val is not None:
			params += ' --{} {}'.format(key,val)
# Execute command
	cmd = 'env |grep PATH'
	cmd = 'ls -l /usr/local/bin | grep combi'
	cmd = '/usr/local/espa-surface-reflectance/lasrc/bin/combine_l8_aux_data --help'
	cmd = 'updatelads.py --help'
	cmd = 'cp /usr/local/espa-surface-reflectance/lasrc/bin/combine_l8_aux_data /app'
	cmd = './combine_l8_aux_data --help'
	cmd = '/app/combine_l8_aux_data --help'
	cmd = 'updatelads.py --help'
	cmd = 'python updatelads.py {}'.format(params)
	app.logger.warning('updatelads - calling cmd {} '.format(cmd))
	try:
		ret = subprocess.call(cmd, shell = True)
		#ret = subprocess.check_output(cmd, stderr=subprocess.PIPE, shell=True)
		#ret = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=False)
		#return '{}\n'.format(ret)
	except subprocess.CalledProcessError as e:
		#options['message'] = e
		app.logger.warning('updatelads - subprocess error {}'.format(e))
		return jsonify(options)
	return jsonify(options)

@app.route('/espa')
def espa():
	activity = {}
	for key in request.args:
		activity[key] = request.args.get(key)
	inputFull = '/home/espa/input-data' + activity['file']
	app.logger.warning('espa - inputFull {}  activity {}'.format(inputFull,activity))
	activity['start'] = request.args.get('start', None)
	step_start = time.time()
	if not os.path.exists(inputFull):
		activity['status'] = 'ERROR'
		activity['message'] = 'No such file {}'.format(inputFull)
		activity['retcode'] = 1
		step_end = time.time()
		elapsedtime = step_end - step_start
		activity['end'] = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(step_end)))
		activity['elapsed'] = str(datetime.timedelta(seconds=elapsedtime))
		app.logger.warning('espa - ERROR in activity {}'.format(activity))
		return jsonify(activity)

# Decode input file name and encode output product id and order id (pathrow)
	inputproductid = os.path.basename(inputFull).replace('.tar.gz','')
	cc = inputproductid.split('_')
	outputproductid = cc[0]+cc[2]+cc[3]
	pathrow = cc[2]
	yyyymm = cc[3][:4]+'-'+cc[3][4:6]

# Output product dir 
	productdir = '/home/espa/output-data/{}/{}'.format(yyyymm,pathrow)
# Build command
	cmd = 'cli.py \
 --dev-mode \
 --debug \
 --product-type landsat \
 --output-format gtiff \
 --include-pixel-qa \
 --include-sr-evi \
 --include-sr-ndvi \
 --include-sr-savi \
 --include-surface-reflectance'
	cmd += ' --order-id {}'.format(pathrow) 
	cmd += ' --input-product-id {}'.format(inputproductid) 
	cmd += ' --input-url file://{}'.format(inputFull) 
	cmd += ' --dist-dir /home/espa/output-data/{}'.format(yyyymm) 
	#cmd += ' --work-dir /home/espa/work-dir/LC8/{}'.format(yyyymm) 
	app.logger.warning('espa - cmd {}'.format(cmd))

# cli.py creates the output file in dist-dir/order-id

# Check if output file already exists
	template = '{}/{}*.tar.gz'.format(productdir,outputproductid)
	app.logger.warning('espa - output file {} '.format(template))
	ofl = glob.glob(template)
	retcode = 0
	if len(ofl) > 0:
		app.logger.warning('espa - {} already exists'.format(template))
	
# Execute command
	#cmd = 'cli.py --help'
	app.logger.warning('espa - calling cmd {} '.format(cmd))
	try:
		subprocess.check_output(cmd, stderr=subprocess.PIPE, shell=True)
	except subprocess.CalledProcessError as e:
		retcode = e.returncode
		activity['message'] = 'Abnormal execution'
		activity['status'] = 'ERROR'
		activity['retcode'] = 1
		app.logger.warning('espa - subprocess error {}'.format(e))
		return jsonify(activity)
	#retcode = subprocess.call(cmd, shell = True)
	ofl = glob.glob(template)
	if len(ofl) > 0:
		tar = tarfile.open(ofl[0], 'r:gz')
		tar.extractall(productdir)
	step_end = time.time()
	elapsedtime = step_end - step_start
	activity['end'] = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(step_end)))
	activity['elapsed'] = str(datetime.timedelta(seconds=elapsedtime))
	if retcode == 0:
		activity['message'] = 'Normal execution'
		activity['status'] = 'DONE'
	else:
		activity['message'] = 'Abnormal execution'
		activity['status'] = 'ERROR'
	activity['retcode'] = 0
	return jsonify(activity)

if __name__ == "__main__":
	app.run(host="0.0.0.0", port=5032, debug=True)
