import boto3
import bs4
import datetime
import fnmatch
import glob
import io
import json
import logging
import os
import numpy
import random
import requests
import scipy
import shutil
import skimage
import sqlalchemy
import tarfile
import time
import threading
import utils
import zipfile

from flask import Flask, request, make_response, render_template, abort, jsonify
from flask_cors import CORS
from numpngw import write_png
from osgeo import gdal, osr, ogr
from osgeo.gdalconst import *
from skimage import exposure
from skimage.transform import resize
from redis import Redis, RedisError
from utils import c2jyd,do_insert,do_update,do_upsert,do_query,do_command,decodePeriods,decodePathRow
from usgs import api, USGSError


app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})
app.config['PROPAGATE_EXCEPTIONS'] = True
app.logger_name = "maestro"
handler = logging.FileHandler('maestro.log')
handler.setFormatter(logging.Formatter(
	'[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
))

app.logger.addHandler(handler)
app.jinja_env.trim_blocks = True
app.jinja_env.lstrip_blocks = True
app.jinja_env.keep_trailing_newline = True

app.logger.warning('connect to Redis.')
redis = Redis(host="redis", db=0)
app.logger.warning('connected to Redis.')

MAX_THREADS = int(os.environ.get('MAX_THREADS'))
CUR_THREADS = 0
SESSION = None
S3Client = None
CLOUD_DEFAULT = 90

s2users = {}

ACTIVITIES = {}
###################################################
def setActivities():
	global ACTIVITIES
	ACTIVITIES = {'uploadS2':{'current':0,'maximum':0},'publishS2':{'current':0,'maximum':0},'publishLC8':{'current':0,'maximum':0},'downloadS2':{'current':0,'maximum':10},'downloadLC8':{'current':0,'maximum':4},'sen2cor':{'current':0,'maximum':2},'espa':{'current':0,'maximum':0}}
	app.logger.warning('Activities set as: {}'.format(ACTIVITIES))
	return('Activities were set')
setActivities()

###################################################
def getLock():
# Manage lock
	app.logger.warning('getLock from Redis.')
	lock = None
	try:
		lock = redis.get('rc_lock')
	except RedisError:
		app.logger.exception('Cannot connect to Redis.')
		return 0
	if lock is None:
		redis.set('rc_lock',0)

	return redis.get('rc_lock')

################################
def createWkt(ullon,ullat,lrlon,lrlat):

# Create ring
	ring = ogr.Geometry(ogr.wkbLinearRing)
	ring.AddPoint(ullon, ullat)
	ring.AddPoint(lrlon, ullat)
	ring.AddPoint(lrlon, lrlat)
	ring.AddPoint(ullon, lrlat)
	ring.AddPoint(ullon, ullat)

# Create polygon
	poly = ogr.Geometry(ogr.wkbPolygon)
	poly.AddGeometry(ring)

	return poly.ExportToWkt(),poly



################################
def downloadLC8(scene):
	global SESSION
	app.logger.warning('downloadLC8 - scene {}'.format(scene))
	cc = scene['sceneid'].split('_')
	pathrow = cc[2]
	yyyymm = cc[3][:4]+'-'+cc[3][4:6]
# Output product dir 
	productdir = '/LC8/{}/{}'.format(yyyymm,pathrow)
	if not os.path.exists(productdir):
		os.makedirs(productdir)
	
	link = scene['link']
	app.logger.warning('downloadLC8 - link {}'.format(link))
	getSESSION()
	r = SESSION.get(link, stream=True)
	app.logger.warning('downloadLC8 - r {}'.format(r.headers))
	count = 0
	while r.headers.get("Content-Disposition") is None and count < 2:
		app.logger.warning('downloadLC8 - Content-Disposition not found for {}'.format(link))
		count += 1
		cc = link.split('/')
		sid = cc[-3]
		last = ord(sid[-1])+1
		last = chr(last)
		cc[-3] = sid[:-1]+last
		link = '/'.join(cc)
		r = SESSION.get(link, stream=True)	
	if count == 2:
		return None
	outtar = os.path.join(productdir, r.headers.get("Content-Disposition").split('=')[1])
	app.logger.warning('downloadLC8 - outtar {}'.format(outtar))
	if r.headers.get("Content-length") is None:
		app.logger.warning('downloadLC8 - Content-Length not found for {}'.format(link))
		return None
	total_size = int(r.headers.get("Content-length"))
	app.logger.warning( 'downloadLC8 - {} to {} size {}'.format(link,outtar,int(total_size/1024/1024)))
	file_size = 0
	if os.path.exists(outtar):
		file_size = os.path.getsize(outtar)
		app.logger.warning( 'downloadLC8 - {} to {} link_size {} file_size {}'.format(link,outtar,total_size,file_size))
	if total_size == file_size:
		app.logger.warning( 'downloadLC8 - {} already downloaded'.format(link))
		return outtar

	block_size = 1024*10
	part = 0
	with open(outtar, 'wb') as fs:
		for chunk in r.iter_content(chunk_size=block_size):
			if chunk:
				fs.write(chunk)
				part += block_size
	return outtar

################################
def developmentSeed(wlon,nlat,elon,slat,startdate,enddate,cloud,limit):
	areathreshold = 5.
	query = 'https://api.developmentseed.org/satellites/?search='


	params = 'satellite_name:landsat-8'
	if enddate is None:
		enddate = datetime.datetime.now().strftime("%Y-%m-%d")
	params += '+AND+acquisitionDate:[%s+TO+%s]' % (startdate, enddate)
	params += '+AND+cloud_coverage:[-1+TO+%s]' % cloud

	qbbox = '+AND+upperLeftCornerLatitude:[{}+TO+1000]+AND+lowerRightCornerLatitude:[-1000+TO+{}]' \
		'+AND+lowerLeftCornerLongitude:[-1000+TO+{}]+AND+upperRightCornerLongitude:[{}+TO+1000]'.format(
			slat, nlat, elon, wlon)
	params += qbbox
	pquery = query + params
	pquery += '&limit={0}'.format(limit)
	app.logger.warning('pquery - '+pquery)

	r = requests.get(pquery)

	scenes = {}
	r_dict = json.loads(r.text)
	#app.logger.warning('r_dict - {}'.format(json.dumps(r_dict, indent=2)))
	if 'results' in r_dict:
		for val in r_dict['results']:
			identifier = val['product_id']
			scenes[identifier] = {}
			scenes[identifier]['sceneid'] = identifier
			scenes[identifier]['scene_id'] = val['scene_id']
			scenes[identifier]['cloud'] = val['cloud_coverage']
			scenes[identifier]['date'] = val['acquisitionDate']
			scenes[identifier]['wlon'] = min(float(val['upperLeftCornerLongitude'])	,float(val['lowerLeftCornerLongitude']))
			scenes[identifier]['elon'] = max(float(val['upperRightCornerLongitude']),float(val['lowerRightCornerLongitude']))
			scenes[identifier]['slat'] = min(float(val['lowerLeftCornerLatitude']),float(val['lowerRightCornerLatitude']))
			scenes[identifier]['nlat'] = max(float(val['upperLeftCornerLatitude']),float(val['upperRightCornerLatitude']))
			scenes[identifier]['path'] = int(val['path'])				
			scenes[identifier]['row'] = int(val['row'])	
			scenes[identifier]['resolution'] = int(val['GRID_CELL_SIZE_REFLECTIVE'])	
# Get file names
			#download_url = api.download('LANDSAT_8', 'EE', [val['scene_id']], api_key=api_key)
			scenes[identifier]['link'] = val['download_links']['usgs']
			scenes[identifier]['icon'] = val['aws_thumbnail']
			#scenes[identifier]['link'] = download_url
	return scenes

#########################################
def developmentSeed_sat_api(wlon,nlat,elon,slat,startdate,enddate,cloud,limit):
	collection='landsat-8-l1'

	if enddate is None:
		enddate = datetime.datetime.now().strftime("%Y-%m-%d")
	if limit is None:
		limit = 299
	
	url = 'https://sat-api.developmentseed.org/stac/search'
	params = {
		"bbox": [
			wlon,
			slat,
			elon,
			nlat
		],
		"time": "{}T00:00:00Z/{}T23:59:59Z".format(startdate, enddate),
		"limit": "{}".format(limit),
		"query": {
			"eo:cloud_cover": {"lt": cloud},
			"collection": {"eq": "{}".format(collection)}
		}
	}
	app.logger.warning('query - {} - {}'.format(url, json.dumps(params)))
	r = requests.post(url, data= json.dumps(params))
	r_dict = r.json()

	scenes = {}
	### Check if request obtained results
	if(r_dict['meta']['returned'] > 0):
		for i in range(len(r_dict['features'])):
			### This is performed due to BAD catalog, which includes box from -170 to +175 (instead of -)
			if ( (r_dict['features'][i]['bbox'][0] - r_dict['features'][i]['bbox'][2]) > -3 ):
				identifier = r_dict['features'][i]['properties']['landsat:product_id'] # CHECK L1TP L1GT
				scenes[identifier] = {}
				scenes[identifier]['sceneid'] = identifier
				scenes[identifier]['scene_id'] = r_dict['features'][i]['id']
				scenes[identifier]['cloud'] = int(r_dict['features'][i]['properties']['eo:cloud_cover'])
				scenes[identifier]['date'] = r_dict['features'][i]['properties']['datetime'][:10]
				scenes[identifier]['wlon'] = float(r_dict['features'][i]['bbox'][0])
				scenes[identifier]['slat'] = float(r_dict['features'][i]['bbox'][1])
				scenes[identifier]['elon'] = float(r_dict['features'][i]['bbox'][2])
				scenes[identifier]['nlat'] = float(r_dict['features'][i]['bbox'][3])
				scenes[identifier]['path'] = int(r_dict['features'][i]['properties']['eo:column'])
				scenes[identifier]['row'] = int(r_dict['features'][i]['properties']['eo:row'])
				scenes[identifier]['resolution'] = r_dict['features'][i]['properties']['eo:bands'][3]['gsd']
				if ( str(r_dict['features'][i]['id']).find('LGN00') != -1 ):
					scenes[identifier]['scene_id'] = r_dict['features'][i]['id']
				else:
					scenes[identifier]['scene_id'] = '{}LGN00'.format( r_dict['features'][i]['id'] )
				scenes[identifier]['link'] = 'https://earthexplorer.usgs.gov/download/12864/{}/STANDARD/EE'.format(scenes[identifier]['scene_id'])
				scenes[identifier]['icon'] = r_dict['features'][i]['assets']['thumbnail']['href']
	return scenes

#########################################
def espaDone(scene):
	identifier = scene['sceneid']
	cc = scene['sceneid'].split('_')
	pathrow = cc[2]
	date = cc[3]
	yyyymm = cc[3][:4]+'-'+cc[3][4:6]
# Product dir 
	productdir = '/LC8SR/{}/{}'.format(yyyymm,pathrow)
	template = productdir+'/LC08_*_{}_{}_*.tif'.format(pathrow,date)
	fs = glob.glob(template)
	app.logger.warning('espaDone - productdir {} files {}'.format(productdir,len(fs)))
	if len(fs) > 0:
		return True
	return False
	
#########################################
def publishLC8(scene):
	identifier = scene['sceneid']
	cc = scene['sceneid'].split('_')
	pathrow = cc[2]
	date = cc[3]
	yyyymm = cc[3][:4]+'-'+cc[3][4:6]
# Product dir 
	productdir = '/LC8SR/{}/{}'.format(yyyymm,pathrow)
	Type='SCENE'
	GeometricProcessing='ortho'
	RadiometricProcessing='SR'
	path = int(pathrow[0:3])
	row  = int(pathrow[3:])

	result = {'Scene':{},'Product':{}}
	result['Scene']['SceneId'] = identifier
	result['Scene']['Dataset'] = 'LC8SR'
	result['Scene']['Satellite'] = 'LC8'
	result['Scene']['Sensor'] = 'OLI'
	result['Scene']['Date'] = date
	result['Scene']['Path'] = path					
	result['Scene']['Row'] = row		

	result['Product']['SceneId'] = identifier
	result['Product']['Dataset'] = 'LC8SR'
	result['Product']['Type'] = 'SCENE'
	result['Product']['GeometricProcessing'] = GeometricProcessing
	result['Product']['RadiometricProcessing'] = RadiometricProcessing

# Connect to db and delete all data about this scene
	connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('CATALOG_USER'),
											  os.environ.get('CATALOG_PASS'),
											  os.environ.get('CATALOG_HOST'),
											  'catalogo')
	engine = sqlalchemy.create_engine(connection)
	sql = "DELETE FROM Scene WHERE SceneId = '{0}'".format(identifier)
	engine.execute(sql)
	sql = "DELETE FROM Product WHERE SceneId = '{0}'".format(identifier)
	engine.execute(sql)
	sql = "DELETE FROM Qlook WHERE SceneId = '{0}'".format(identifier)
	engine.execute(sql)

# Get the product files
	bandmap= {		
			'coastal': 'sr_band1',
			'blue': 'sr_band2',
			'green': 'sr_band3',
			'red': 'sr_band4',
			'nir': 'sr_band5',
			'swir1': 'sr_band6',
			'swir2': 'sr_band7',
			'evi': 'sr_evi',
			'ndvi': 'sr_ndvi',
			'quality': 'pixel_qa'
		}
	quicklook = ["swir2","nir","red"]

	files = {}
	qlfiles = {}
	for gband in bandmap:
		band = bandmap[gband]
		template = productdir+'/LC08_*_{}_{}_*_{}.tif'.format(pathrow,date,band)
		fs = glob.glob(template)
		files[gband] = fs[0]
		if gband in quicklook:
			qlfiles[gband] = fs[0]

	app.logger.warning('publishLC8 - productdir {} files {}'.format(productdir,len(files)))
# Extract basic scene information and build the quicklook
	pngname = productdir+'/{}.png'.format(identifier)
	pngexists = False
	if os.path.exists(pngname): pngexists = True
	dataset = gdal.Open(qlfiles['nir'],GA_ReadOnly)
	numlin = 768
	numcol = int(float(dataset.RasterXSize)/float(dataset.RasterYSize)*numlin)
	image = numpy.zeros((numlin,numcol,len(qlfiles),), dtype=numpy.uint8)
	geotransform = dataset.GetGeoTransform()
	projection = dataset.GetProjection()
	datasetsrs = osr.SpatialReference()
	datasetsrs.ImportFromWkt(projection)

# Extract bounding box and resolution
	RasterXSize = dataset.RasterXSize
	RasterYSize = dataset.RasterYSize

	resolutionx = geotransform[1]
	resolutiony = geotransform[5]
	fllx = fulx = geotransform[0]
	fury = fuly = geotransform[3]
	furx = flrx = fulx + resolutionx * RasterXSize
	flly = flry = fuly + resolutiony * RasterYSize

# Create transformation from scene to ll coordinate

	llsrs = osr.SpatialReference()
	llsrs.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')
	s2ll = osr.CoordinateTransformation ( datasetsrs, llsrs )

# Evaluate corners coordinates in ll
#	Upper left corner
	(ullon, ullat, nkulz ) = s2ll.TransformPoint( fulx, fuly)
#	Upper right corner
	(urlon, urlat, nkurz ) = s2ll.TransformPoint( furx, fury)
#	Lower left corner
	(lllon, lllat, nkllz ) = s2ll.TransformPoint( fllx, flly)
#	Lower right corner
	(lrlon, lrlat, nklrz ) = s2ll.TransformPoint( flrx, flry)

	result['Scene']['CenterLatitude'] = (ullat+lrlat+urlat+lllat)/4.
	result['Scene']['CenterLongitude'] = (ullon+lrlon+urlon+lllon)/4.

	result['Scene']['TL_LONGITUDE'] = ullon
	result['Scene']['TL_LATITUDE'] = ullat

	result['Scene']['BR_LONGITUDE'] = lrlon
	result['Scene']['BR_LATITUDE'] = lrlat

	result['Scene']['TR_LONGITUDE'] = urlon
	result['Scene']['TR_LATITUDE'] = urlat

	result['Scene']['BL_LONGITUDE'] = lllon
	result['Scene']['BL_LATITUDE'] = lllat

	result['Scene']['IngestDate'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")	
	result['Scene']['Deleted'] = 0
	result['Scene']['CloudCoverMethod'] = 'M'
	result['Scene']['CloudCoverQ1'] = 0
	result['Scene']['CloudCoverQ2'] = 0
	result['Scene']['CloudCoverQ3'] = 0
	result['Scene']['CloudCoverQ4'] = 0

	nb = 0
	for band in quicklook:
		template = qlfiles[band]
		dataset = gdal.Open(template,GA_ReadOnly)
		raster = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)
		app.logger.warning('publishLC8 - file {} raster before min {} max {} {}'.format(template,raster.min(),raster.max(),raster))
		#raster = scipy.misc.imresize(raster,(numlin,numcol))
		raster = resize(raster,(numlin,numcol), order=1, preserve_range=True)
		nodata = raster == -9999
		app.logger.warning('publishLC8 - file {} raster after min {} max {} {}'.format(template,raster.min(),raster.max(),raster))
# Evaluate minimum and maximum values
		a = numpy.array(raster.flatten())
		p1, p99 = numpy.percentile(a[a>0], (1, 99))
# Convert minimum and maximum values to 1,255 - 0 is nodata
		raster = exposure.rescale_intensity(raster, in_range=(p1, p99),out_range=(1,255)).astype(numpy.uint8)
		#app.logger.warning('publishLC8 - band {} p1 {} p99 {}'.format(band,p1,p99))
		image[:,:,nb] = raster.astype(numpy.uint8) * numpy.invert(nodata)
		nb += 1
	write_png(pngname, image, transparent=(0, 0, 0))


# Inserting data into Scene table
	params = ''
	values = ''
	for key,val in result['Scene'].items():
		params += key+','
		if type(val) is str:
				values += "'{0}',".format(val)
		else:
				values += "{0},".format(val)

	sql = "INSERT INTO Scene ({0}) VALUES({1})".format(params[:-1],values[:-1])
	engine.execute(sql)

# Inserting data into Qlook table
	sql = "INSERT INTO  Qlook (SceneId,QLfilename) VALUES('%s', '%s')" % (identifier, pngname)
	engine.execute(sql)

# Inserting data into Product table
	for band in bandmap:
		template = files[band]
		result['Product']['Band'] = band
		result['Product']['Filename'] = template
		dataset = gdal.Open(template,GA_ReadOnly)
		geotransform = dataset.GetGeoTransform()
		result['Product']['Resolution'] = geotransform[1]
		ProcessingDate = datetime.datetime.fromtimestamp(os.path.getctime(template)).strftime('%Y-%m-%d %H:%M:%S')
		result['Product']['ProcessingDate'] = ProcessingDate
		params = ''
		values = ''
		for key,val in result['Product'].items():
			params += key+','
			if type(val) is str:
					values += "'{0}',".format(val)
			else:
					values += "{0},".format(val)

		sql = "INSERT INTO Product ({0}) VALUES({1})".format(params[:-1],values[:-1])
		engine.execute(sql)
	engine.dispose()
	return 0

################################
def is_valid(zfile):
	try:
		archive = zipfile.ZipFile(zfile, 'r')
		try:
			corrupt = True if archive.testzip() else False
		except zlib.error:
			corrupt = True
		archive.close()
	except zipfile.BadZipfile:
		corrupt = True
	return not corrupt

################################
def extractall(zfile):
	uzfile = zfile.replace('.zip','.SAFE')
	if os.path.exists(uzfile): return
	archive = zipfile.ZipFile(zfile, 'r')
	archive.extractall(os.path.dirname(zfile))
	archive.close()

################################
def downloadS2(scene):
	cc = scene['sceneid'].split('_')
	yyyymm = cc[2][:4]+'-'+cc[2][4:6]
# Output product dir 
	productdir = '/S2_MSI/{}'.format(yyyymm)
	link = scene['link']
	sceneId = scene['sceneid']
	if not os.path.exists(productdir):
		os.makedirs(productdir)
	zfile = productdir + '/' + sceneId + '.zip'
	safeL1Cfull = productdir + '/' + sceneId + '.SAFE'
	
	app.logger.warning('downloadS2 - link {} file {}'.format(link,zfile))
	if not os.path.exists(safeL1Cfull):
		valid = True
		if os.path.exists(zfile):
			valid = is_valid(zfile)
		if not os.path.exists(zfile) or not valid:
			status = doDownloadS2(link,zfile)
			if not status:
				return None

			"""
			try:
				if not os.path.exists("secrets_s2.csv"):
			    	return 'No secrets_s2.csv'
				fh = open('secrets_s2.csv','r')
				line = fh.readline()
				line = fh.readline()
				line = line.strip()
				cc = line.split(",")

				s2_user = str(cc[0])
				s2_pass = str(cc[1])
				response = requests.get(link, auth=(s2_user, s2_pass), stream=True)
			except requests.exceptions.ConnectionError:
				app.logger.exception('downloadS2 - Connection Error')
				return None
			if 'Content-Length' not in response.headers:
				app.logger.exception('downloadS2 - Content-Length not found')
				return None
			size = int(response.headers['Content-Length'].strip())
			app.logger.warning('downloadS2 - {} size {} MB'.format(zfile,int(size/1024/1024)))
			down = open(zfile, 'wb')
			for buf in response.iter_content(1024):
				if buf:
					down.write(buf)
			down.close()
			"""
# Check if file is valid
			valid = is_valid(zfile)

		if not valid:
			os.remove(zfile)
			return None
		else:
			extractall(zfile)

	return safeL1Cfull

###################################################
def getS2Users():
	global s2users
	if len(s2users) == 0:
		if not os.path.exists("secrets_S2.JSON"):
		    return 'No secrets_S2.JSON'
		fh = open('secrets_S2.JSON','r')
		s2users = json.load(fh)
	return 2*len(s2users)

###################################################
def doDownloadS2(link,zfile):
	global s2users
	getS2Users()
	user = None
	for s2user in s2users:
		if s2users[s2user]['count'] < 2:
			user = s2user
			s2users[user]['count'] += 1
			break
	if user is None:
		app.logger.warning('doDownloadS2 - nouser')
		return False

	app.logger.warning('doDownloadS2 - user {} link {}'.format(user,link))
	try:
		response = requests.get(link, auth=(user, s2users[user]['password']), stream=True)
	except requests.exceptions.ConnectionError:
		app.logger.warning('doDownloadS2 - Connection Error')
		s2users[user]['count'] -= 1
		return False
	if 'Content-Length' not in response.headers:
		app.logger.warning('doDownloadS2 - Content-Length not found for user {} in {} {}'.format(user,link,response.text))
		s2users[user]['count'] -= 1
		return False
	size = int(response.headers['Content-Length'].strip())
	if size < 30*1024*1024:
		app.logger.warning('doDownloadS2 - user {} {} size {} MB too small'.format(user,zfile,int(size/1024/1024)))
		s2users[user]['count'] -= 1
		return False
	app.logger.warning('doDownloadS2 - user {} {} size {} MB'.format(user,zfile,int(size/1024/1024)))
	down = open(zfile, 'wb')

	for buf in response.iter_content(1024):
		if buf:
			down.write(buf)

	down.close()
	s2users[user]['count'] -= 1
	return True


###################################################
def openSearchS2SAFE(wlon,nlat,elon,slat,startdate,enddate,cloud,limit,productType=None):

#    api_hub options:
#    'https://scihub.copernicus.eu/apihub/' for fast access to recently acquired imagery in the API HUB rolling archive
#    'https://scihub.copernicus.eu/dhus/' for slower access to the full archive of all acquired imagery
	scenes = {}
	totres = 1000000
	first = 0
	pquery = 'https://scihub.copernicus.eu/dhus/search?format=json'
	pquery = 'https://scihub.copernicus.eu/apihub/search?format=json'
	pquery += '&q=platformname:Sentinel-2'
	if productType is not None:
		pquery += ' AND producttype:{}'.format(productType)
	if enddate is None:
		enddate = datetime.datetime.now().strftime("%Y-%m-%d")
	pquery += ' AND beginposition:[{}T00:00:00.000Z TO {}T23:59:59.999Z]'.format(startdate,enddate)
	pquery += ' AND cloudcoverpercentage:[0 TO {}]'.format(cloud)
	if wlon == elon and slat == nlat:
		pfootprintWkt,footprintPoly = createWkt(wlon-0.01,nlat+0.01,elon+0.01,slat-0.01)
		pquery += ' AND (footprint:"Contains({})")'.format(footprintPoly)
	else:
		pfootprintWkt,footprintPoly = createWkt(wlon,nlat,elon,slat)
		pquery += ' AND (footprint:"Intersects({})")'.format(footprintPoly)
	
	limit = int(limit)
	rows = min(100,limit)
	count_results = 0
	while count_results < min(limit,totres) and totres != 0:
		rows = min(100,limit-len(scenes),totres)
		first = count_results
		query = pquery + '&rows={}&start={}'.format(rows,first)
		app.logger.warning('openSearchS2SAFE {}'.format(query))
		try:
			if not os.path.exists("secrets_s2.csv"):
			    return 'No secrets_s2.csv'
			fh = open('secrets_s2.csv','r')
			line = fh.readline()
			line = fh.readline()
			line = line.strip()
			cc = line.split(",")

			s2_user = str(cc[0])
			s2_pass = str(cc[1])
			r = requests.get(query, auth=(s2_user, s2_pass), verify=True)
			if not r.status_code // 100 == 2:
				app.logger.exception('openSearchS2SAFE API returned unexpected response {}:'.format(r.status_code))
				return {}
			r_dict = r.json()
			#app.logger.warning('r_dict - {}'.format(json.dumps(r_dict, indent=2)))

		except requests.exceptions.RequestException as exc:
			app.logger.exception('openSearchS2SAFE error {}:'.format(exc))
			return {}

		if 'entry' in r_dict['feed']:
			totres = int(r_dict['feed']['opensearch:totalResults'])
			app.logger.warning('Results for this feed: {}'.format(totres))
			results = r_dict['feed']['entry']
			#logging.warning('Results: {}'.format(results))
			if not isinstance(results, list):
				results = [results]
			for result in results:
				count_results += 1
				identifier = result['title']
				type = identifier.split('_')[1]
				date = identifier.split('_')[2][:8]
				if date > '20181220' and type == 'MSIL1C': 
					app.logger.warning('openSearchS2SAFE skipping {}'.format(identifier))
					continue
				scenes[identifier] = {}
				scenes[identifier]['pathrow'] = identifier.split('_')[-2][1:]
				scenes[identifier]['sceneid'] = identifier
				scenes[identifier]['type'] = identifier.split('_')[1]
				for data in result['date']:
					if str(data['name']) == 'beginposition':
						scenes[identifier]['date'] = str(data['content'])[0:10]
				if not isinstance(result['double'], list):
					result['double'] = [result['double']]
				for data in result['double']:
					if str(data['name']) == 'cloudcoverpercentage':
						scenes[identifier]['cloud'] = float(data['content'])
				for data in result['str']:
					if str(data['name']) == 'size':
						scenes[identifier]['size'] = data['content']
					if str(data['name']) == 'footprint':
						scenes[identifier]['footprint'] = data['content']
					if str(data['name']) == 'tileid':
						scenes[identifier]['tileid'] = data['content']
				if 'tileid' not in scenes[identifier]:
					app.logger.warning( 'openSearchS2SAFE identifier - {} - tileid {} was not found'.format(identifier,scenes[identifier]['pathrow']))
					app.logger.warning(json.dumps(scenes[identifier], indent=4))
				scenes[identifier]['link'] = result['link'][0]['href']
				scenes[identifier]['icon'] = result['link'][2]['href']
		else:
			app.logger.warning('openSearchS2SAFE - No results for this feed')
			totres = 0
	return scenes


#########################################
def publishAsCOG(identifier,productdir,sband,jp2file,alreadyTiled=False):
	app.logger.warning('function:publishAsCOG')
	cogfile = os.path.join(productdir,identifier+'_'+sband+'.tif')
	if os.path.exists(cogfile):
		return cogfile
	driver = gdal.GetDriverByName('GTiff')
	dataset = gdal.Open(jp2file,GA_ReadOnly)
	dst_ds = driver.CreateCopy(cogfile, dataset,  options = [ 'COMPRESS=LZW', 'TILED=YES'  ] )
	gdal.SetConfigOption('COMPRESS_OVERVIEW', 'LZW')
	dst_ds.BuildOverviews('NEAREST', [2, 4, 8, 16, 32])
	dst_ds = None
	return cogfile

#########################################
def publishAsTif(identifier,productdir,sband,jp2file):
	if os.path.splitext(jp2file)[1] == '.tif': return jp2file
	tiffile = os.path.join(productdir,identifier+'_'+sband+'.tif')
	if os.path.exists(tiffile):
		return tiffile
	
	dataset = gdal.Open(jp2file,GA_ReadOnly)
	raster = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)
	driver = gdal.GetDriverByName('GTiff')
	tifdataset = driver.Create( tiffile, dataset.RasterXSize, dataset.RasterYSize, 1, gdal.GDT_Int16,  options = [ 'COMPRESS=LZW', 'TILED=YES'  ] )
	tifdataset.SetGeoTransform(dataset.GetGeoTransform())
	tifdataset.SetProjection(dataset.GetProjection())
	tifdataset.GetRasterBand(1).WriteArray( raster )
	tifdataset.GetRasterBand(1).SetNoDataValue(0)
	dataset = None
	tifdataset = None
	return tiffile

################################
def publishS2(scene):
	app.logger.warning('function:publishS2')
	sbands = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B09', 'B10', 'B11', 'B12', 'SCL']
	qlband = 'TCI'
	bandmap = { \
"B01":"coastal", \
"B02":"blue", \
"B03":"green", \
"B04":"red", \
"B05":"redge1", \
"B06":"redge2", \
"B07":"redge3", \
"B08":"bnir", \
"B8A":"nir", \
"B09":"wvap", \
"B10":"cirrus", \
"B11":"swir1", \
"B12":"swir2", \
"SCL":"quality" \
}
# Basic information about scene
# S2B_MSIL1C_20180731T131239_N0206_R138_T24MTS_20180731T182838
	sceneId = os.path.basename(scene['file'])
	parts = sceneId.split('_')
	sat = parts[0]
	inst = parts[1][0:3]
	date = parts[2][0:8]
	calendardate = date[0:4]+'-'+date[4:6]+'-'+date[6:8]
	yyyymm = date[0:4]+'_'+date[4:6]
	tile = parts[5]
	identifier = sceneId.split('.')[0].replace('MSIL1C','MSIL2A')

# Create metadata structure and start filling metadata structure for tables Scene and Product in Catalogo database
	result = {'Scene':{},'Product':{}}
	result['Scene']['SceneId'] = str(identifier)
	result['Scene']['Dataset'] = 'S2SR'
	result['Scene']['Satellite'] = sat
	result['Scene']['Sensor'] = inst
	result['Scene']['Date'] = calendardate
	result['Scene']['Path'] = 0					
	result['Scene']['Row'] = 0		
	result['Product']['Dataset'] = 'S2SR'
	result['Product']['Type'] = 'SCENE'
	result['Product']['RadiometricProcessing'] = 'SR'
	result['Product']['ProcessingDate'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	result['Product']['GeometricProcessing'] = 'ortho'
	result['Product']['SceneId'] = str(identifier)

# Find all jp2 files in L2A SAFE
	safeL2Afull = scene['file'].replace('MSIL1C','MSIL2A')
	template =  "T*.jp2"
	jp2files = [os.path.join(dirpath, f)
		for dirpath, dirnames, files in os.walk("{0}".format(safeL2Afull))
		for f in fnmatch.filter(files, template)]
	if 	len(jp2files) <= 1:
		app.logger.warning( 'publishS2 - No {} files found in {}'.format(template,safeL2Afull))
		template =  "L2A_T*.jp2"
		jp2files = [os.path.join(dirpath, f)
			for dirpath, dirnames, files in os.walk("{0}".format(safeL2Afull))
			for f in fnmatch.filter(files, template)]
		if 	len(jp2files) <= 1:
			app.logger.warning( 'publishS2 - No {} files found in {}'.format(template,safeL2Afull))
			return 1
	app.logger.warning('publishS2 - safeL2Afull {} found {} files template {}'.format(safeL2Afull,len(jp2files),template))

# Find the desired files to be published and put then in files 
	bands = []
	files = {}
	for jp2file in sorted(jp2files):
		filename = os.path.basename(jp2file)
		parts = filename.split('_')
		band = parts[-2]
		if band not in bands and band in sbands:
			bands.append(band)
			files[bandmap[band]] = jp2file
		elif band == qlband:
			files['qlfile'] = jp2file

# Define new filenames for products
	app.logger.warning('publishS2 - qlfile {}'.format(files['qlfile']))
	parts = os.path.basename(files['qlfile']).split('_')
	filebasename = '_'.join(parts[:-2])
	parts = files['qlfile'].split('/')
	productdir = '/'.join(parts[:-2])

# Create vegetation index
	app.logger.warning('Generate Vegetation index')
	if generateVI(filebasename,productdir,files) != 0:
		app.logger.warning('Vegetation index != 0')
		return 1
	bands.append('NDVI')
	bands.append('EVI')
	bandmap['NDVI'] = 'ndvi'
	bandmap['EVI'] = 'evi'

# Convert original format to COG
	productdir = '/'.join(parts[:4])
	productdir += '/PUBLISHED'
	if not os.path.exists(productdir):
		os.makedirs(productdir)
	for sband in bands:
		band = bandmap[sband]
		file = files[band]
		app.logger.warning('publishS2 - COG band {} sband {} file {}'.format(band,sband,file))
		files[band] = publishAsCOG(filebasename,productdir,sband,file)
		
# Create Qlook file
	qlfile =  files['qlfile']
	pngname = os.path.join(productdir,filebasename+'.png')
	app.logger.warning('publishS2 - pngname {}'.format(pngname))
	if not os.path.exists(pngname):
		image = numpy.ones((768,768,3,), dtype=numpy.uint8)
		dataset = gdal.Open(qlfile,GA_ReadOnly)
		for nb in [0,1,2]:
			raster = dataset.GetRasterBand(nb+1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)
			image[:,:,nb] = resize(raster, (768,768), order=1, preserve_range=True).astype(numpy.uint8)
			write_png(pngname, image, transparent=(0, 0, 0))
	qlfile =  pngname

# Extract basic parameters from quality file
	file = files['quality']
	dataset = gdal.Open(file,GA_ReadOnly)
	raster = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)
	geotransform = dataset.GetGeoTransform()
	projection = dataset.GetProjection()
	datasetsrs = osr.SpatialReference()
	datasetsrs.ImportFromWkt(projection)

# Extract bounding box and resolution
	app.logger.warning('extract bb and resolution')
	RasterXSize = dataset.RasterXSize
	RasterYSize = dataset.RasterYSize

	resolutionx = geotransform[1]
	resolutiony = geotransform[5]
	fllx = fulx = geotransform[0]
	fury = fuly = geotransform[3]
	furx = flrx = fulx + resolutionx * RasterXSize
	flly = flry = fuly + resolutiony * RasterYSize

# Create transformation from files to ll coordinate
	llsrs = osr.SpatialReference()
	llsrs.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')
	s2ll = osr.CoordinateTransformation ( datasetsrs, llsrs )

# Evaluate corners coordinates in ll
#	Upper left corner
	(ullon, ullat, nkulz ) = s2ll.TransformPoint( fulx, fuly)
#	Upper right corner
	(urlon, urlat, nkurz ) = s2ll.TransformPoint( furx, fury)
#	Lower left corner
	(lllon, lllat, nkllz ) = s2ll.TransformPoint( fllx, flly)
#	Lower right corner
	(lrlon, lrlat, nklrz ) = s2ll.TransformPoint( flrx, flry)

	result['Scene']['CenterLatitude'] = (ullat+lrlat+urlat+lllat)/4.
	result['Scene']['CenterLongitude'] = (ullon+lrlon+urlon+lllon)/4.

	result['Scene']['TL_LONGITUDE'] = ullon
	result['Scene']['TL_LATITUDE'] = ullat

	result['Scene']['BR_LONGITUDE'] = lrlon
	result['Scene']['BR_LATITUDE'] = lrlat

	result['Scene']['TR_LONGITUDE'] = urlon
	result['Scene']['TR_LATITUDE'] = urlat

	result['Scene']['BL_LONGITUDE'] = lllon
	result['Scene']['BL_LATITUDE'] = lllat

	result['Scene']['IngestDate'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")	
	result['Scene']['Deleted'] = 0

# Compute cloud cover
	"""
	Label Classification
	0		NO_DATA
	1		SATURATED_OR_DEFECTIVE
	2		DARK_AREA_PIXELS
	3		CLOUD_SHADOWS
	4		VEGETATION
	5		BARE_SOILS
	6		WATER
	7		CLOUD_LOW_PROBABILITY
	8		CLOUD_MEDIUM_PROBABILITY
	9		CLOUD_HIGH_PROBABILITY
	10		THIN_CIRRUS
	11		SNOW
	"""
	unique, counts = numpy.unique(raster, return_counts=True)
	clear = 0.
	cloud = 0.
	for i in range(0,unique.shape[0]):
		if unique[i] == 0:
			continue
		elif unique[i] in [1,2,3,8,9,10]:
			cloud += float(counts[i])
		else:
			clear += float(counts[i])
	cloudcover = int(round(100.*cloud/(clear+cloud),0))
	app.logger.warning('publishS2 - cloudcover {}'.format(cloudcover))

	result['Scene']['CloudCoverMethod'] = 'A'
	result['Scene']['CloudCoverQ1'] = cloudcover
	result['Scene']['CloudCoverQ2'] = cloudcover
	result['Scene']['CloudCoverQ3'] = cloudcover
	result['Scene']['CloudCoverQ4'] = cloudcover

# Connect to db and delete all data about this scene
	connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('CATALOG_USER'),
											  os.environ.get('CATALOG_PASS'),
											  os.environ.get('CATALOG_HOST'),
											  'catalogo')
	engine = sqlalchemy.create_engine(connection)
	sql = "DELETE FROM Scene WHERE SceneId = '{0}'".format(identifier)
	engine.execute(sql)
	sql = "DELETE FROM Product WHERE SceneId = '{0}'".format(identifier)
	engine.execute(sql)
	sql = "DELETE FROM Qlook WHERE SceneId = '{0}'".format(identifier)
	engine.execute(sql)

# Inserting data into Scene table
	params = ''
	values = ''
	for key,val in result['Scene'].items():
		params += key+','
		if type(val) is str:
				values += "'{0}',".format(val)
		else:
				values += "{0},".format(val)
		
	sql = "INSERT INTO Scene ({0}) VALUES({1})".format(params[:-1],values[:-1])
	app.logger.warning('publishS2 - sql {}'.format(sql))
	engine.execute(sql)

# Inserting data into Qlook table
	sql = "INSERT INTO Qlook (SceneId,QLfilename) VALUES('%s', '%s')" % (identifier, qlfile)
	app.logger.warning('publishS2 - sql {}'.format(sql))
	engine.execute(sql)

# Inserting data into Product table
	for sband in bands:
		band = bandmap[sband]
		file = files[band]
		ProcessingDate = datetime.datetime.fromtimestamp(os.path.getctime(file)).strftime('%Y-%m-%d %H:%M:%S')
		result['Product']['ProcessingDate'] = ProcessingDate
		dataset = gdal.Open(file,GA_ReadOnly)
		if dataset is None:
			app.logger.warning('publishS2 - file {} is corrupted'.format(file))
			continue
		geotransform = dataset.GetGeoTransform()
		result['Product']['Resolution'] = geotransform[1]
		result['Product']['Band'] = band
		result['Product']['Filename'] = file
		params = ''
		values = ''
		for key,val in result['Product'].items():
				params += key+','
				if type(val) is str:
						values += "'{0}',".format(val)
				else:
						values += "{0},".format(val)
		sql = "INSERT INTO Product ({0}) VALUES({1})".format(params[:-1],values[:-1])
		app.logger.warning('publishS2 - sql {}'.format(sql))
		engine.execute(sql)
	engine.dispose()
	return 0

################################
def publishS2_old(scene):
	sbands = ['AOT', 'B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B09', 'B10', 'B11', 'B12', 'DEM', 'SCL', 'WVP']
	qlband = 'TCI'
	bandmap = { \
"B01":"coastal", \
"B02":"blue", \
"B03":"green", \
"B04":"red", \
"B05":"redge1", \
"B06":"redge2", \
"B07":"redge3", \
"B08":"nir", \
"B8A":"nnir", \
"B09":"wvap", \
"B10":"cirrus", \
"B11":"swir1", \
"B12":"swir2", \
"AOT":"AOT", \
"WVP":"WVP", \
"DEM":"DEM", \
"SCL":"quality" \
}
# Basic information about scene
# S2B_MSIL1C_20180731T131239_N0206_R138_T24MTS_20180731T182838
	sceneId = os.path.basename(scene['file'])
	parts = sceneId.split('_')
	sat = parts[0]
	inst = parts[1][0:3]
	date = parts[2][0:8]
	calendardate = date[0:4]+'-'+date[4:6]+'-'+date[6:8]
	yyyymm = date[0:4]+'_'+date[4:6]
	tile = parts[5]
	identifier = parts[0]+'_'+parts[1][0:3]+'L2A_'+date+'_'+parts[3]+'_'+parts[4]+'_'+tile
	identifier = sceneId.split('.')[0]
	productdir = '/S2SR/'+yyyymm+'/'+tile

# Create output directory
	app.logger.warning('publishS2 - productdir {}'.format(productdir))
	if not os.path.exists(productdir):
		os.makedirs(productdir)

# Create metadata structure and start filling metadata structure for tables Scene and Product in Catalogo database
	result = {'Scene':{},'Product':{}}
	result['Scene']['SceneId'] = str(identifier)
	result['Scene']['Dataset'] = 'S2SR'
	result['Scene']['Satellite'] = sat
	result['Scene']['Sensor'] = inst
	result['Scene']['Date'] = calendardate
	result['Scene']['Path'] = 0					
	result['Scene']['Row'] = 0		
	result['Scene']['Orbit'] = 0		
	result['Product']['Dataset'] = 'S2SR'
	result['Product']['Type'] = 'SCENE'
	result['Product']['RadiometricProcessing'] = 'SR'
	result['Product']['ProcessingDate'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	result['Product']['GeometricProcessing'] = 'ortho'
	result['Product']['SceneId'] = str(identifier)

# Find all jp2 files in L2A SAFE
	safeL2Afull = scene['file'].replace('MSIL1C','MSIL2A')
	template =  "T*.jp2"
	jp2files = [os.path.join(dirpath, f)
		for dirpath, dirnames, files in os.walk("{0}".format(safeL2Afull))
		for f in fnmatch.filter(files, template)]
	if 	len(jp2files) <= 1:
		app.logger.warning( 'publishS2 - No {} files found in {}'.format(template,safeL2Afull))
		template =  "L2A_T*.jp2"
		jp2files = [os.path.join(dirpath, f)
			for dirpath, dirnames, files in os.walk("{0}".format(safeL2Afull))
			for f in fnmatch.filter(files, template)]
		if 	len(jp2files) <= 1:
			app.logger.warning( 'publishS2 - No {} files found in {}'.format(template,safeL2Afull))
			return 1
	bands = []
	files = {}
	app.logger.warning('publishS2 - safeL2Afull {} found {} files template {}'.format(safeL2Afull,len(jp2files),template))

# Find the desired files to be published and put then in files 
	for jp2file in sorted(jp2files):
		filename = os.path.basename(jp2file)
		parts = filename.split('_')
		band = parts[-2]
		if band not in bands and band in sbands:
			bands.append(band)
			files[bandmap[band]] = jp2file
		elif band == qlband:
			files['qlfile'] = jp2file

# Publish original products
	for sband in bands:
		band = bandmap[sband]
		file = files[band]
		newfile = publishAsTif(identifier,productdir,sband,file)

# Create vegetation index
	if generateVI(identifier,productdir,files) != 0:
		return 1
	bands.append('NDVI')
	bands.append('EVI')
	bandmap['NDVI'] = 'ndvi'
	bandmap['EVI'] = 'evi'

# Create Qlook file
	qlfile =  files['qlfile']
	pngname = os.path.join(productdir,identifier+'.png')
	app.logger.warning('publishS2 - pngname {}'.format(pngname))
	if not os.path.exists(pngname):
		image = numpy.ones((768,768,3,), dtype=numpy.uint8)
		dataset = gdal.Open(qlfile,GA_ReadOnly)
		for nb in [0,1,2]:
			raster = dataset.GetRasterBand(nb+1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)
			image[:,:,nb] = resize(raster, (768,768), order=1, preserve_range=True).astype(numpy.uint8)
			write_png(pngname, image, transparent=(0, 0, 0))
	qlfile =  pngname

# Extract basic parameters from quality file
	file = files['quality']
	dataset = gdal.Open(file,GA_ReadOnly)
	raster = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)
	geotransform = dataset.GetGeoTransform()
	projection = dataset.GetProjection()
	datasetsrs = osr.SpatialReference()
	datasetsrs.ImportFromWkt(projection)

# Extract bounding box and resolution
	RasterXSize = dataset.RasterXSize
	RasterYSize = dataset.RasterYSize

	resolutionx = geotransform[1]
	resolutiony = geotransform[5]
	fllx = fulx = geotransform[0]
	fury = fuly = geotransform[3]
	furx = flrx = fulx + resolutionx * RasterXSize
	flly = flry = fuly + resolutiony * RasterYSize

# Create transformation from files to ll coordinate
	llsrs = osr.SpatialReference()
	llsrs.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')
	s2ll = osr.CoordinateTransformation ( datasetsrs, llsrs )

# Evaluate corners coordinates in ll
#	Upper left corner
	(ullon, ullat, nkulz ) = s2ll.TransformPoint( fulx, fuly)
#	Upper right corner
	(urlon, urlat, nkurz ) = s2ll.TransformPoint( furx, fury)
#	Lower left corner
	(lllon, lllat, nkllz ) = s2ll.TransformPoint( fllx, flly)
#	Lower right corner
	(lrlon, lrlat, nklrz ) = s2ll.TransformPoint( flrx, flry)

	result['Scene']['CenterLatitude'] = (ullat+lrlat+urlat+lllat)/4.
	result['Scene']['CenterLongitude'] = (ullon+lrlon+urlon+lllon)/4.

	result['Scene']['TL_LONGITUDE'] = ullon
	result['Scene']['TL_LATITUDE'] = ullat

	result['Scene']['BR_LONGITUDE'] = lrlon
	result['Scene']['BR_LATITUDE'] = lrlat

	result['Scene']['TR_LONGITUDE'] = urlon
	result['Scene']['TR_LATITUDE'] = urlat

	result['Scene']['BL_LONGITUDE'] = lllon
	result['Scene']['BL_LATITUDE'] = lllat

	result['Scene']['IngestDate'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")	
	result['Scene']['Deleted'] = 0

# Compute cloud cover
	"""
	Label Classification
	0		NO_DATA
	1		SATURATED_OR_DEFECTIVE
	2		DARK_AREA_PIXELS
	3		CLOUD_SHADOWS
	4		VEGETATION
	5		BARE_SOILS
	6		WATER
	7		CLOUD_LOW_PROBABILITY
	8		CLOUD_MEDIUM_PROBABILITY
	9		CLOUD_HIGH_PROBABILITY
	10		THIN_CIRRUS
	11		SNOW
	"""
	unique, counts = numpy.unique(raster, return_counts=True)
	clear = 0.
	cloud = 0.
	for i in range(0,unique.shape[0]):
		if unique[i] == 0:
			continue
		elif unique[i] in [1,2,3,8,9,10]:
			cloud += float(counts[i])
		else:
			clear += float(counts[i])
	cloudcover = int(round(100.*cloud/(clear+cloud),0))
	app.logger.warning('publishS2 - cloudcover {}'.format(cloudcover))

	result['Scene']['CloudCoverMethod'] = 'A'
	result['Scene']['CloudCoverQ1'] = cloudcover
	result['Scene']['CloudCoverQ2'] = cloudcover
	result['Scene']['CloudCoverQ3'] = cloudcover
	result['Scene']['CloudCoverQ4'] = cloudcover

# Connect to db and delete all data about this scene
	connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('CATALOG_USER'),
											  os.environ.get('CATALOG_PASS'),
											  os.environ.get('CATALOG_HOST'),
											  'catalogo')
	engine = sqlalchemy.create_engine(connection)
	sql = "DELETE FROM Scene WHERE SceneId = '{0}'".format(identifier)
	engine.execute(sql)
	sql = "DELETE FROM Product WHERE SceneId = '{0}'".format(identifier)
	engine.execute(sql)
	sql = "DELETE FROM Qlook WHERE SceneId = '{0}'".format(identifier)
	engine.execute(sql)

# Inserting data into Scene table
	params = ''
	values = ''
	for key,val in result['Scene'].items():
		params += key+','
		if type(val) is str:
				values += "'{0}',".format(val)
		else:
				values += "{0},".format(val)
		
	sql = "INSERT INTO Scene ({0}) VALUES({1})".format(params[:-1],values[:-1])
	app.logger.warning('publishS2 - sql {}'.format(sql))
	engine.execute(sql)

# Inserting data into Qlook table
	sql = "INSERT INTO Qlook (SceneId,QLfilename) VALUES('%s', '%s')" % (identifier, qlfile)
	app.logger.warning('publishS2 - sql {}'.format(sql))
	engine.execute(sql)

# Inserting data into Product table
	for sband in bands:
		band = bandmap[sband]
		file = files[band]
		newfile = publishAsTif(identifier,productdir,sband,file)
		if os.path.exists(file):
			ProcessingDate = datetime.datetime.fromtimestamp(os.path.getctime(file)).strftime('%Y-%m-%d %H:%M:%S')
			result['Product']['ProcessingDate'] = ProcessingDate
			dataset = gdal.Open(newfile,GA_ReadOnly)
			if dataset is None:
				app.logger.warning('publishS2 - newfile {} is corrupted'.format(newfile))
				continue
			geotransform = dataset.GetGeoTransform()
			result['Product']['Resolution'] = geotransform[1]
		else:
			app.logger.warning('publishS2 - newfile {} does not exist'.format(newfile))
			continue
		result['Product']['Band'] = band
		result['Product']['Filename'] = newfile
		params = ''
		values = ''
		for key,val in result['Product'].items():
				params += key+','
				if type(val) is str:
						values += "'{0}',".format(val)
				else:
						values += "{0},".format(val)
		sql = "INSERT INTO Product ({0}) VALUES({1})".format(params[:-1],values[:-1])
		app.logger.warning('publishS2 - sql {}'.format(sql))
		engine.execute(sql)
	engine.dispose()
	return 0


#########################################
def generateVI(identifier,productdir,files):
	ndviname = os.path.join(productdir,identifier+"_NDVI.tif")
	eviname = os.path.join(productdir,identifier+"_EVI.tif")
	app.logger.warning('generateVI - ndviname {}'.format(ndviname))
	app.logger.warning('generateVI - eviname {}'.format(eviname))
	app.logger.warning('generateVI - nir {}'.format(files['nir']))
	app.logger.warning('generateVI - red {}'.format(files['red']))
	app.logger.warning('generateVI - blue {}'.format(files['blue']))
	files['ndvi'] = ndviname
	files['evi'] = eviname
	if os.path.exists(ndviname) and os.path.exists(eviname):
		app.logger.warning('generateVI returning 0 cause ndvi and evi exists')
		return 0

	app.logger.warning('open red band, read band')
	step_start = time.time()
	dataset = gdal.Open(files['red'],GA_ReadOnly)
	RasterXSize = dataset.RasterXSize
	RasterYSize = dataset.RasterYSize
	red = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize).astype(numpy.float32)/10000.
	app.logger.warning('open nir band, read band')
	dataset = gdal.Open(files['nir'],GA_ReadOnly)
	nir = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize).astype(numpy.float32)/10000.
	app.logger.warning('resize')
	nir = resize(nir,red.shape, order=1, preserve_range=True).astype(numpy.float32)
	app.logger.warning('open blue band, read band')
	dataset = gdal.Open(files['blue'],GA_ReadOnly)
	blue = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize).astype(numpy.float32)/10000.

# Create the ndvi image dataset if it not exists
	app.logger.warning('Create the ndvi image dataset if it not exists')
	driver = gdal.GetDriverByName('GTiff')
	if not os.path.exists(ndviname):
		rasterndvi = (10000 * (nir - red) / (nir + red + 0.0001)).astype(numpy.int16)
		#rasterndvi[rasterndvi<=0] = 0
		app.logger.warning('generateVI - ndviname {} shape {} {} {}'.format(ndviname,rasterndvi.shape,dataset.RasterXSize, dataset.RasterYSize))
		ndvidataset = driver.Create( ndviname, RasterXSize, RasterYSize, 1, gdal.GDT_Int16,  options = [ 'COMPRESS=LZW', 'TILED=YES' ] )
		ndvidataset.SetGeoTransform(dataset.GetGeoTransform())
		ndvidataset.SetProjection(dataset.GetProjection())
		ndvidataset.GetRasterBand(1).WriteArray( rasterndvi )
		#ndvidataset.GetRasterBand(1).SetNoDataValue(0)
		rasterndvi = None
		ndvidataset = None
	
# Create the evi image dataset if it not exists
	app.logger.warning('Create the evi image dataset if it not exists')
	if not os.path.exists(eviname):
		evidataset = driver.Create( eviname, RasterXSize, RasterYSize, 1, gdal.GDT_Int16,  options = [ 'COMPRESS=LZW', 'TILED=YES'  ] )
		rasterevi = (10000 * 2.5 * (nir - red)/(nir + 6. * red - 7.5 * blue + 1)).astype(numpy.int16)
		app.logger.warning('generateVI - eviname {} shape {} {} {}'.format(eviname,rasterevi.shape,dataset.RasterXSize, dataset.RasterYSize))
		#rasterevi[rasterevi<=0] = 0
		evidataset.SetGeoTransform(dataset.GetGeoTransform())
		evidataset.SetProjection(dataset.GetProjection())
		evidataset.GetRasterBand(1).WriteArray( rasterevi )
		#evidataset.GetRasterBand(1).SetNoDataValue(0)
		rasterevi = None
		evidataset = None
	dataset = nir = red = blue = None
	elapsedtime = time.time() - step_start
	ela = str(datetime.timedelta(seconds=elapsedtime))
	app.logger.warning('create VI returning 0 Ok')
	return 0

###################################################
@app.route('/checkdup', methods=['GET','POST'])
def checkdup():
	global S3Client,bucket_name
	getS3Client()
	start = request.args.get('start', '2018-12-10')
# Connect to db and delete all data about this scene
	connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('CATALOG_USER'),
											  os.environ.get('CATALOG_PASS'),
											  os.environ.get('CATALOG_HOST'),
											  'catalogo')
	engine = sqlalchemy.create_engine(connection)
	sql = "SELECT * FROM Scene WHERE Date > '{0}' AND Dataset = 'S2SR' ORDER BY SceneId DESC".format(start)
	scenes = engine.execute(sql)
	scenes = scenes.fetchall()
	scenes = [dict(scene) for scene in scenes]
	logging.warning('checkdup - '+sql+' - Rows: {}'.format(len(scenes)))
	scenesperid = {}
	for scene in scenes:
#    "SceneId": "S2A_MSIL2A_20190512T131251_N0212_R138_T23KPT_20190512T152952", 
		SceneId = scene['SceneId']
		parts = SceneId.split('_')
		logging.warning('checkdup - SceneId: {} parts {}'.format(SceneId,parts))
		date = parts[2]
		tile = parts[5]
		id = tile+date
		if id not in scenesperid:
			scenesperid[id] = 0
		else:
			sql = "SELECT * FROM Product WHERE SceneId = '{0}'".format(SceneId)
			products = engine.execute(sql)
			products = products.fetchall()
			products = [dict(product) for product in products]
			for product in products[0:1]:
				dir = product['Filename'].split('/')
				dir = '/'.join(dir[:-2])
				logging.warning('checkdup - dir {}'.format(dir))
# Delete dir
				if os.path.exists(dir):
					shutil.rmtree(dir, ignore_errors=True)
				sql = "DELETE FROM Product WHERE SceneId = '{0}'".format(SceneId)
				engine.execute(sql)
				sql = "DELETE FROM Qlook WHERE SceneId = '{0}'".format(SceneId)
				engine.execute(sql)
				result = S3Client.list_objects_v2(Bucket=bucket_name, Prefix=dir[1:])
				logging.warning('checkdup S3 result {} '.format(result))
				object_names = []
				if 'Contents' in result:
					#S3Client.delete_objects(Bucket=bucket_name,Delete={'Objects': [{'Key':dir[1:],}]},RequestPayer='requester')
					for obj in result['Contents']:
						logging.warning('checkdup S3 dir {} '.format(obj.get('Key')))
						object_names.append(obj.get('Key'))
					objlist = [{'Key': obj} for obj in object_names]
					S3Client.delete_objects(Bucket=bucket_name, Delete={'Objects': objlist})
	engine.dispose()
	return jsonify(scenesperid)
	 	
###################################################
@app.route('/espaAll', methods=['GET','POST'])
def espaAll():
# Find all LC8SR files in /LC8
	LC8SRdir = '/LC8'
# LC08_L1TP_222068_20160728_20180528_01_T1.tar.gz
	template = request.args.get('template', None)
	if template is None:
		template =  "LC08_*tar.gz"
	app.logger.warning('espaAll - find {} -name {}'.format(LC8SRdir,template))
	LC8SRfiles = [os.path.join(dirpath, f)
		for dirpath, dirnames, files in os.walk("{0}".format(LC8SRdir))
		for f in fnmatch.filter(files, template)]
	count = {'espa' : 0, 'publishLC8' : 0}
	for file in LC8SRfiles:
		activity = {}
		activity['sceneid'] = os.path.basename(file).replace('.tar.gz','')
		activity['file'] = file
		activity['id'] = None
		if not espaDone(activity):
			activity['app'] = 'espa'
			activity['priority'] = 2
			count['espa'] += 1
		else:
			activity['app'] = 'publishLC8'
			activity['priority'] = 3
			count['publishLC8'] += 1
		activity['status'] = 'NOTDONE'
		activity['message'] = ''
		activity['retcode'] = 0
		app.logger.warning('espaAll - activity {}'.format(activity))
		do_upsert('activities',activity,['id','status','link','start','end','elapsed','retcode','message'])
	#start()
	return jsonify(count)


###################################################
@app.route('/sameday', methods=['GET','POST'])
def sameday():

# Get the product
	year = request.args.get('year', None)
	start = request.args.get('start', None)
	end = request.args.get('end', None)

# Find all Modis files in /Modis/year
	HLSDir = '/Repository/Archive/HLS'
	if year is not None:
		HLSDir += '/'+year
	mgrs = request.args.get('mgrs', None)
# HLS.L30.T23LLF.2013107.v1.4
	template =  '*.hdf'
	if mgrs is not None:
		template =  '*{}*.hdf'.format(mgrs)
	app.logger.warning('sameday - find {} -name {}'.format(HLSDir,template))
	scenes = {}
	HLSfiles = [os.path.join(dirpath, f)
		for dirpath, dirnames, files in os.walk("{0}".format(HLSDir))
		for f in fnmatch.filter(files, template)]
	for file in sorted(HLSfiles):
		sceneid = os.path.basename(file).replace('.hdf','')
		hdfdate = sceneid[15:22]
		(cddate,y,yyyymm) = j2cyd(hdfdate)
		if cddate not in scenes:
			scenes[cddate] = []
		scenes[cddate].append(os.path.basename(file))
	fscenes = {}
	for cddate in scenes:
		if len(scenes[cddate]) > 1:
			fscenes[cddate] = scenes[cddate]
			sceneid = scenes[cddate][0]
			hdfdate = sceneid[15:22]
			tileid = sceneid.split('.')[2]
			(scddate,y,yyyymm) = j2cyd(hdfdate)
			scddate = scddate.replace('-','')
# Find L2A SAFE
			safeL2Afull = '/Repository/Archive/S2_MSI/'+yyyymm
			template =  safeL2Afull+'/S*MSIL2A_{}*{}*.SAFE'.format(scddate,tileid)
			app.logger.warning('sameday - find {} -name {}'.format(safeL2Afull,template))
			safes = glob.glob(template)
			if len(safes) > 0:
				fscenes[cddate].append(os.path.basename(safes[0]))
# Find LC08_L1TP_218069_20180706_20180717_01_T1.png
			LC8SRfull = '/Repository/Archive/LC8SR/'+yyyymm
			template =  'LC08_*{}*.png'.format(scddate)
			app.logger.warning('sameday - find {} -name {}'.format(LC8SRfull,template))
			LC8SRfiles = [os.path.join(dirpath, f)
				for dirpath, dirnames, files in os.walk("{0}".format(LC8SRfull))
				for f in fnmatch.filter(files, template)]
			for  LC8SRfile in LC8SRfiles:
				fscenes[cddate].append(os.path.basename(LC8SRfile))

	return jsonify(fscenes)

###################################################
@app.route('/publisHLS', methods=['GET','POST'])
def publisHLS():
# wget -r -nd -np -nc -l1 -A '*.hdf' https://hls.gsfc.nasa.gov/data/v1.4/L30/2019/23/L/M/F
# Get the product
	product = request.args.get('product', 'HLS.L30')
	year = request.args.get('year', None)
	start = request.args.get('start', None)
	end = request.args.get('end', None)

# Find all Modis files in /Modis/year
	HLSDir = '/Repository/Archive/HLS'
	if year is not None:
		HLSDir += '/'+year
	mgrs = request.args.get('mgrs', None)
# HLS.L30.T23LLF.2013107.v1.4
	template =  product+'*.hdf'
	if mgrs is not None:
		template =  product+'*{}*.hdf'.format(mgrs)
	app.logger.warning('publishHLS - find {} -name {}'.format(HLSDir,template))
	HLSfiles = [os.path.join(dirpath, f)
		for dirpath, dirnames, files in os.walk("{0}".format(HLSDir))
		for f in fnmatch.filter(files, template)]
	for file in sorted(HLSfiles):
		scene = {}
		sceneid = os.path.basename(file).replace('.hdf','')
		hdfdate = sceneid[15:22]
		(cddate,y,yyyymm) = j2cyd(hdfdate)
		app.logger.warning('publishHLS - {} date {}'.format(file,cddate))
		if start is not None and cddate < start: continue
		if end is not None and cddate > end: continue
		scene = {}
		scene['sceneid'] = sceneid
		scene['basename'] = os.path.basename(file)
		scene['hdffile'] = file
		scene['product'] = product
		publishOneHLS(scene)
	return jsonify(HLSfiles)

###################################################
def publishOneHLS(scene):
	hdffile = scene['hdffile']
	bands = {}
	bands['HLS.L30'] = { 
		0 : 'coastal',
		1 : 'blue', 
		2 : 'green', 
		3 : 'red', 
		4 : 'nir', 
		5 : 'swir1', 
		6 : 'swir2',
		10 : 'quality' }

	bands['HLS.S30'] = { 
		0 : 'coastal',
		1 : 'blue', 
		2 : 'green', 
		3 : 'red', 
		4 : 'redge1', 
		5 : 'redge2', 
		6 : 'redge3', 
		7 : 'bnir', 
		8 : 'nir', 
		11 : 'swir1', 
		12 : 'swir2',
		13 : 'quality' }

	product = scene['product']
	hdfdate = scene['basename'][15:22]
	(cddate,y,yyyymm) = j2cyd(hdfdate)
	identifier = scene['sceneid']
	mgrs = os.path.basename(hdffile).split('.')[-4][1:]
	app.logger.warning('publishOneHLS - identifier {} mgrs {} cddate {} product {} bands {}'.format(identifier,mgrs,cddate,product,bands[product]))
	dataset = gdal.Open(hdffile,GA_ReadOnly)
	subdatasets = dataset.GetSubDatasets()
	driver = gdal.GetDriverByName('GTiff')
	scene['tiffiles'] = []
	tiffilesmap = {}
	tifdir = '/Repository/Archive/{}/{}/{}'.format(scene['product'],yyyymm,scene['sceneid'])
	if not os.path.exists(tifdir):
		os.makedirs(tifdir)
	scene['tifdir'] = tifdir
	for band in bands[product]:
		sband = bands[product][band]
		subdataset = subdatasets[band]
		tiffile = os.path.join(tifdir,'{}_{}.tif'.format(scene['sceneid'],sband))
		scene['tiffiles'].append(tiffile)
		tiffilesmap[band] = tiffile
		if os.path.exists(tiffile): continue
		app.logger.warning('publishOneHLS - tiffile {} subdataset {} '.format(tiffile,subdataset))
		subdataset = gdal.Open(subdataset[0],GA_ReadOnly)
		dst_ds = driver.CreateCopy(tiffile, subdataset,  options = [ 'COMPRESS=LZW', 'TILED=YES'  ] )

# Generate QuickLook
	qlbands = [6,4,3]
	numlin = numcol = 768
	nb = 0
	pngname = os.path.join(tifdir,'{}.png'.format(scene['sceneid']))
	scene['tiffiles'].append(pngname)
	if not os.path.exists(pngname):
		image = numpy.zeros((numlin,numcol,3,), dtype=numpy.uint8)
		for band in qlbands:
			subdataset = gdal.Open(subdatasets[band][0],GA_ReadOnly)
			raster = subdataset.GetRasterBand(1).ReadAsArray(0, 0, subdataset.RasterXSize, subdataset.RasterYSize)
			raster = resize(raster,(numlin,numcol), order=1, preserve_range=True)
			nodata = raster < 0
	# Evaluate minimum and maximum values
			a = numpy.array(raster.flatten())
			p1, p99 = numpy.percentile(a[a>=0], (1, 99))
	# Convert minimum and maximum values to 1,255 - 0 is nodata
			raster = exposure.rescale_intensity(raster, in_range=(p1, p99),out_range=(1,255)).astype(numpy.uint8)
			image[:,:,nb] = raster.astype(numpy.uint8) * numpy.invert(nodata)
			nb += 1
		write_png(pngname, image, transparent=(0, 0, 0))

# Create metadata structure and start filling metadata structure for tables Scene and Product in Catalogo database

	result = {'Scene':{},'Product':{}}
	result['Scene']['SceneId'] = str(identifier)
	result['Scene']['Dataset'] = scene['product']
	result['Scene']['Satellite'] = 'LC8' if identifier.find('HLS.L3') == 0 else 'S2'
	result['Scene']['Sensor'] = 'OLI' if identifier.find('HLS.L3') == 0 else 'MSI'
	result['Scene']['Date'] = cddate
	result['Scene']['Path'] = -1			
	result['Scene']['Row'] = -1	
	result['Product']['Dataset'] = scene['product']
	result['Product']['Type'] = 'SCENE'
	result['Product']['RadiometricProcessing'] = 'SR'
	result['Product']['ProcessingDate'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	result['Product']['GeometricProcessing'] = 'ortho'
	result['Product']['SceneId'] = str(identifier)

	subdataset = gdal.Open(subdatasets[0][0],GA_ReadOnly)
	subdataset_tags = subdataset.GetMetadata('') 
	app.logger.warning('publishOneHLS - identifier {} subdataset_tags {}'.format(identifier,subdataset_tags))
	mgeotransform = subdataset.GetGeoTransform()
	mRasterXSize = subdataset.RasterXSize
	mRasterYSize = subdataset.RasterYSize
	mprojection = subdataset.GetProjection()
	subdatasetsrs = osr.SpatialReference()
	subdatasetsrs.ImportFromWkt(mprojection)
# Extract bounding box and resolution
	resolutionx = mgeotransform[1]
	resolutiony = mgeotransform[5]
	fllx = fulx = mgeotransform[0]
	fury = fuly = mgeotransform[3]
	furx = flrx = fulx + resolutionx * mRasterXSize
	flly = flry = fuly + resolutiony * mRasterYSize

# Create transformation from scene to ll coordinate

	llsrs = osr.SpatialReference()
	llsrs.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')
	s2ll = osr.CoordinateTransformation ( subdatasetsrs, llsrs )

	# Evaluate corners coordinates in ll
	#	Upper left corner
	(ullon, ullat, nkulz ) = s2ll.TransformPoint( fulx, fuly)
	#	Upper right corner
	(urlon, urlat, nkurz ) = s2ll.TransformPoint( furx, fury)
	#	Lower left corner
	(lllon, lllat, nkllz ) = s2ll.TransformPoint( fllx, flly)
	#	Lower right corner
	(lrlon, lrlat, nklrz ) = s2ll.TransformPoint( flrx, flry)

	result['Scene']['CenterLatitude'] = (ullat+lrlat+urlat+lllat)/4.
	result['Scene']['CenterLongitude'] = (ullon+lrlon+urlon+lllon)/4.

	result['Scene']['TL_LONGITUDE'] = ullon
	result['Scene']['TL_LATITUDE'] = ullat

	result['Scene']['BR_LONGITUDE'] = lrlon
	result['Scene']['BR_LATITUDE'] = lrlat

	result['Scene']['TR_LONGITUDE'] = urlon
	result['Scene']['TR_LATITUDE'] = urlat

	result['Scene']['BL_LONGITUDE'] = lllon
	result['Scene']['BL_LATITUDE'] = lllat

	result['Scene']['IngestDate'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")	
	result['Scene']['Deleted'] = 0

	result['Scene']['CloudCoverMethod'] = 'A'
	result['Scene']['CloudCoverQ1'] = subdataset_tags['cloud_coverage']
	result['Scene']['CloudCoverQ2'] = subdataset_tags['cloud_coverage']
	result['Scene']['CloudCoverQ3'] = subdataset_tags['cloud_coverage']
	result['Scene']['CloudCoverQ4'] = subdataset_tags['cloud_coverage']

# Connect to db and delete all data about this scene
	connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('CATALOG_USER'),
											  os.environ.get('CATALOG_PASS'),
											  os.environ.get('CATALOG_HOST'),
											  'catalogo')
	engine = sqlalchemy.create_engine(connection)
	sql = "DELETE FROM Scene WHERE SceneId = '{}'".format(identifier)
	engine.execute(sql)
	sql = "DELETE FROM Product WHERE SceneId = '{}'".format(identifier)
	engine.execute(sql)
	sql = "DELETE FROM Qlook WHERE SceneId = '{}'".format(identifier)
	engine.execute(sql)

# Inserting data into Scene table
	params = ''
	values = ''
	for key,val in result['Scene'].items():
		params += key+','
		if type(val) is str:
				values += "'{0}',".format(val)
		else:
				values += "{0},".format(val)
		
	sql = "INSERT INTO Scene ({0}) VALUES({1})".format(params[:-1],values[:-1])
	app.logger.warning('publishOneHLS - sql {}'.format(sql))
	engine.execute(sql)

# Inserting data into Qlook table
	sql = "INSERT INTO Qlook (SceneId,QLfilename) VALUES('%s', '%s')" % (identifier, pngname.replace('/Repository/Archive',''))
	app.logger.warning('publishOneHLS - sql {}'.format(sql))
	engine.execute(sql)

# Inserting data into Product table
	for band in bands[product]:
		sband = bands[product][band]
		result['Product']['Band'] = sband
		result['Product']['Filename'] = tiffilesmap[band].replace('/Repository/Archive','')
		result['Product']['Resolution'] = resolutionx
		params = ''
		values = ''
		for key,val in result['Product'].items():
				params += key+','
				if type(val) is str:
						values += "'{0}',".format(val)
				else:
						values += "{0},".format(val)
		sql = "INSERT INTO Product ({0}) VALUES({1})".format(params[:-1],values[:-1])
		app.logger.warning('publishOneHLS - sql {}'.format(sql))
		engine.execute(sql)
	engine.dispose()


###################################################
@app.route('/publishMCD', methods=['GET','POST'])
def publishMCD():
# Get the product
	product = request.args.get('product', 'MCD12Q1')
	year = request.args.get('year', None)

# Find all Modis files in /Modis/year
	ModisDir = '/Modis/'+product
	if year is not None:
		ModisDir += '/'+year
	hv = request.args.get('hv', None)
	template =  product+'*.hdf'
	if hv is not None:
		template =  product+'*{}*.hdf'.format(hv)
	app.logger.warning('publishModis - find {} -name {}'.format(ModisDir,template))
#	fp = 'http://www.dpi.inpe.br/newcatalog/tmp/MOD13Q1/2017/MOD13Q1.A2017257.h12v09.006.2017276132700.hdf'
	Modisfiles = [os.path.join(dirpath, f)
		for dirpath, dirnames, files in os.walk("{0}".format(ModisDir))
		for f in fnmatch.filter(files, template)]
	for hdffile in sorted(Modisfiles):
		dataset = gdal.Open(hdffile,GA_ReadOnly)
		subdatasets = dataset.GetSubDatasets()
		classes = {}
		for subdataset in subdatasets:
			app.logger.warning('publishMCD - subdataset {} '.format(subdataset))
			subdataset = gdal.Open(subdataset[0],GA_ReadOnly)
			metadata = subdataset.GetMetadata()
			for key,val in metadata.items():
				if key != 'TileID' and not key.isupper() and val.isnumeric():
					classes[val] = key
		return jsonify(classes)

###################################################
@app.route('/publishModis', methods=['GET','POST'])
def publishModis():

# Get the product
	product = request.args.get('product', 'MOD13Q1')
	year = request.args.get('year', None)
	start = request.args.get('start', None)
	end = request.args.get('end', None)

# Find all Modis files in /Modis/year
	ModisDir = '/Modis/'+product
	#ModisDir = '/{}/'.format(product)
	if year is not None:
		ModisDir += '/'+year

# MOD13Q1.A2017257.h12v09.006.2017276132700.hdf
	hvs = ['h14v09','h14v10','h14v11','h13v08','h13v09','h13v10','h13v11','h13v12','h12v08','h12v09','h12v10','h12v11','h12v12','h11v08','h11v09','h11v10','h11v11','h10v09']
	hv = request.args.get('hv', None)
	if hv is not None:
		hvs = [hv]
	for hv in hvs:
		template =  product+'*.hdf'
		template =  product+'*{}*.hdf'.format(hv)
		app.logger.warning('publishModis - find {} -name {}'.format(ModisDir,template))
		Modisfiles = [os.path.join(dirpath, f)
			for dirpath, dirnames, files in os.walk("{0}".format(ModisDir))
			for f in fnmatch.filter(files, template)]
		for file in sorted(Modisfiles):
			scene = {}
			sceneid = os.path.basename(file).replace('.hdf','')
			hdfdate = sceneid[9:16]
			(cddate,y,yyyymm) = j2cyd(hdfdate)
			app.logger.warning('publishModis - {} date {}'.format(file,cddate))
			if start is not None and cddate < start: continue
			if end is not None and cddate > end: continue
			scene = {}
			scene['sceneid'] = sceneid
			scene['basename'] = os.path.basename(file)
			scene['hdffile'] = file
			scene['product'] = product
			publishOneModis(scene)
			uploadModis(scene)
			app.logger.warning('publishModis - scene {}'.format(scene))
	return jsonify(Modisfiles)


###################################################
def uploadModis(scene):
	global S3Client,bucket_name
	getS3Client()

	prefix = scene['tifdir'][1:] + '/'
	logging.warning('uploadModis S3 prefix {} '.format(prefix))
	s3tiffs = []
	result = S3Client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
	if 'Contents' in result:
		for obj in result['Contents']:
			logging.warning('uploadModis S3 tiff {} '.format(obj.get('Key')))
			s3tiffs.append(os.path.basename(obj.get('Key')))
	count = 0
	for tiff in scene['tiffiles']:
		count += 1
		logging.warning('uploadModis {}/{} - {}'.format(count,len(scene['tiffiles']),tiff))
		if os.path.basename(tiff) in s3tiffs:
			logging.warning('uploadModis {} already in S3'.format(os.path.basename(tiff)))
			continue
		mykey = tiff[1:]
		
		try:
			tc = boto3.s3.transfer.TransferConfig()
			t = boto3.s3.transfer.S3Transfer( client=S3Client, config=tc )
			t.upload_file( tiff, bucket_name, mykey, extra_args={'ACL': 'public-read'})
		except Exception as e:
			logging.warning('uploadModis error {}'.format(e))
			return 1
	return 0

###################################################
def j2cyd(juliandate):
	year = int(juliandate[0:4])
	jday = int(juliandate[4:7])
	dday = datetime.timedelta(days=jday-1)
	dbase = datetime.datetime(year,1,1)
	dbase += dday
	calendardate = dbase.strftime('%Y-%m-%d')
	yyyymm = dbase.strftime('%Y-%m')
	return (calendardate,year,yyyymm)

###################################################
def publishOneModis(scene):
	hdffile = scene['hdffile']
	bands = { 
		0 : 'ndvi',
		1 : 'evi', 
		2 : 'quality', 
		3 : 'red', 
		4 : 'nir', 
		5 : 'blue', 
		6 : 'swir2',
		11 : 'reliability' }
	hdfdate = scene['basename'][9:16]
	(cddate,y,yyyymm) = j2cyd(hdfdate)
	identifier = scene['sceneid']
	row = int(os.path.basename(hdffile)[21:23])
	path  = int(os.path.basename(hdffile)[18:20])
	app.logger.warning('publishOneModis - path {} row {} cddate {}'.format(path,row,cddate))
	dataset = gdal.Open(hdffile,GA_ReadOnly)
	subdatasets = dataset.GetSubDatasets()
	driver = gdal.GetDriverByName('GTiff')
	scene['tiffiles'] = []
	tiffilesmap = {}
	tifdir = '/{}/{}/{}'.format(scene['product'],yyyymm,scene['sceneid'])
	cc = identifier.split('.')
	newidentifier = identifier.replace(cc[1],cddate)
	if not os.path.exists(tifdir):
		os.makedirs(tifdir)
	scene['tifdir'] = tifdir
	for band in bands:
		sband = bands[band]
		subdataset = subdatasets[band]
		tiffile = os.path.join(tifdir,'{}_{}.tif'.format(scene['sceneid'],sband))
		newtiffile = os.path.join(tifdir,'{}_{}.tif'.format(newidentifier,sband))
		if not os.path.exists(newtiffile):
			app.logger.warning('publishOneModis - tiffile {} subdataset {} '.format(tiffile,subdataset))
			subdataset = gdal.Open(subdataset[0],GA_ReadOnly)
			dst_ds = driver.CreateCopy(newtiffile, subdataset,  options = [ 'COMPRESS=LZW', 'TILED=YES'  ] )
			gdal.SetConfigOption('COMPRESS_OVERVIEW', 'LZW')
			dst_ds.BuildOverviews('NEAREST', [2, 4, 8, 16, 32])
			dst_ds = None
			app.logger.warning('publishOneModis - COG band {} sband {} file {}'.format(band,sband,tiffile))
		scene['tiffiles'].append(newtiffile)
		tiffilesmap[band] = newtiffile
		if os.path.exists(tiffile):
			os.remove(tiffile)

# Generate QuickLook
	qlbands = [6,4,3]
	numlin = numcol = 768
	nb = 0
	pngname = os.path.join(tifdir,'{}.png'.format(newidentifier))
	scene['tiffiles'].append(pngname)
	if not os.path.exists(pngname):
		image = numpy.zeros((numlin,numcol,3,), dtype=numpy.uint8)
		for band in qlbands:
			subdataset = gdal.Open(subdatasets[band][0],GA_ReadOnly)
			raster = subdataset.GetRasterBand(1).ReadAsArray(0, 0, subdataset.RasterXSize, subdataset.RasterYSize)
			raster = resize(raster,(numlin,numcol), order=1, preserve_range=True)
			nodata = raster < 0
	# Evaluate minimum and maximum values
			a = numpy.array(raster.flatten())
			p1, p99 = numpy.percentile(a[a>=0], (1, 99))
	# Convert minimum and maximum values to 1,255 - 0 is nodata
			raster = exposure.rescale_intensity(raster, in_range=(p1, p99),out_range=(1,255)).astype(numpy.uint8)
			image[:,:,nb] = raster.astype(numpy.uint8) * numpy.invert(nodata)
			nb += 1
		write_png(pngname, image, transparent=(0, 0, 0))

# Create metadata structure and start filling metadata structure for tables Scene and Product in Catalogo database

	result = {'Scene':{},'Product':{}}
	result['Scene']['SceneId'] = str(identifier)
	result['Scene']['Dataset'] = scene['product']
	result['Scene']['Satellite'] = 'T1' if identifier.find('MOD') == 0 else 'A1'
	result['Scene']['Sensor'] = 'MODIS'
	result['Scene']['Date'] = cddate
	result['Scene']['Path'] = path			
	result['Scene']['Row'] = row	
	result['Product']['Dataset'] = scene['product']
	result['Product']['Type'] = 'MOSAIC'
	result['Product']['RadiometricProcessing'] = 'SR'
	result['Product']['ProcessingDate'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	result['Product']['GeometricProcessing'] = 'ortho'
	result['Product']['SceneId'] = str(identifier)

	subdataset = gdal.Open(subdatasets[0][0],GA_ReadOnly)
	mgeotransform = subdataset.GetGeoTransform()
	mRasterXSize = subdataset.RasterXSize
	mRasterYSize = subdataset.RasterYSize
	mprojection = subdataset.GetProjection()
	subdatasetsrs = osr.SpatialReference()
	subdatasetsrs.ImportFromWkt(mprojection)
# Extract bounding box and resolution
	resolutionx = mgeotransform[1]
	resolutiony = mgeotransform[5]
	fllx = fulx = mgeotransform[0]
	fury = fuly = mgeotransform[3]
	furx = flrx = fulx + resolutionx * mRasterXSize
	flly = flry = fuly + resolutiony * mRasterYSize

# Create transformation from scene to ll coordinate

	llsrs = osr.SpatialReference()
	llsrs.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')
	s2ll = osr.CoordinateTransformation ( subdatasetsrs, llsrs )

	# Evaluate corners coordinates in ll
	#	Upper left corner
	(ullon, ullat, nkulz ) = s2ll.TransformPoint( fulx, fuly)
	#	Upper right corner
	(urlon, urlat, nkurz ) = s2ll.TransformPoint( furx, fury)
	#	Lower left corner
	(lllon, lllat, nkllz ) = s2ll.TransformPoint( fllx, flly)
	#	Lower right corner
	(lrlon, lrlat, nklrz ) = s2ll.TransformPoint( flrx, flry)

	result['Scene']['CenterLatitude'] = (ullat+lrlat+urlat+lllat)/4.
	result['Scene']['CenterLongitude'] = (ullon+lrlon+urlon+lllon)/4.

	result['Scene']['TL_LONGITUDE'] = ullon
	result['Scene']['TL_LATITUDE'] = ullat

	result['Scene']['BR_LONGITUDE'] = lrlon
	result['Scene']['BR_LATITUDE'] = lrlat

	result['Scene']['TR_LONGITUDE'] = urlon
	result['Scene']['TR_LATITUDE'] = urlat

	result['Scene']['BL_LONGITUDE'] = lllon
	result['Scene']['BL_LATITUDE'] = lllat

	result['Scene']['IngestDate'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")	
	result['Scene']['Deleted'] = 0

	result['Scene']['CloudCoverMethod'] = 'A'
	result['Scene']['CloudCoverQ1'] = 0
	result['Scene']['CloudCoverQ2'] = 0
	result['Scene']['CloudCoverQ3'] = 0
	result['Scene']['CloudCoverQ4'] = 0

# Connect to db and delete all data about this scene
	connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('CATALOG_USER'),
											  os.environ.get('CATALOG_PASS'),
											  os.environ.get('CATALOG_HOST'),
											  'catalogo')
	engine = sqlalchemy.create_engine(connection)
	sql = "DELETE FROM Scene WHERE SceneId = '{}'".format(identifier)
	engine.execute(sql)
	sql = "DELETE FROM Product WHERE SceneId = '{}'".format(identifier)
	engine.execute(sql)
	sql = "DELETE FROM Qlook WHERE SceneId = '{}'".format(identifier)
	engine.execute(sql)

# Inserting data into Scene table
	params = ''
	values = ''
	for key,val in result['Scene'].items():
		params += key+','
		if type(val) is str:
				values += "'{0}',".format(val)
		else:
				values += "{0},".format(val)
		
	sql = "INSERT INTO Scene ({0}) VALUES({1})".format(params[:-1],values[:-1])
	app.logger.warning('publishOneModis - sql {}'.format(sql))
	engine.execute(sql)

# Inserting data into Qlook table
	sql = "INSERT INTO Qlook (SceneId,QLfilename) VALUES('%s', '%s')" % (identifier, pngname)
	app.logger.warning('publishOneModis - sql {}'.format(sql))
	engine.execute(sql)

# Inserting data into Product table
	for band in bands:
		sband = bands[band]
		result['Product']['Band'] = sband
		result['Product']['Filename'] = tiffilesmap[band]
		result['Product']['Resolution'] = resolutionx
		params = ''
		values = ''
		for key,val in result['Product'].items():
				params += key+','
				if type(val) is str:
						values += "'{0}',".format(val)
				else:
						values += "{0},".format(val)
		sql = "INSERT INTO Product ({0}) VALUES({1})".format(params[:-1],values[:-1])
		app.logger.warning('publishOneModis - sql {}'.format(sql))
		engine.execute(sql)
	engine.dispose()

###################################################
@app.route('/publishLC8All', methods=['GET','POST'])
def publishLC8All():
# Find all LC8SR files in /LC8SR
	LC8SRdir = '/LC8SR'
# LC08_L1TP_219064_20180729_20180729_01_RT
	template =  "LC08_*pixel_qa.tif"
	app.logger.warning('publishLC8All - find {} -name {}'.format(LC8SRdir,template))
	LC8SRfiles = [os.path.join(dirpath, f)
		for dirpath, dirnames, files in os.walk("{0}".format(LC8SRdir))
		for f in fnmatch.filter(files, template)]
	for file in LC8SRfiles:
		app.logger.warning('publishLC8All - {}'.format(file))
		scene = {}
		scene['sceneid'] = os.path.basename(file).replace('_pixel_qa.tif','')
		publishLC8(scene)
	return jsonify(LC8SRfiles)

###################################################
@app.route('/publishS2All', methods=['GET','POST'])
def publishS2All():
# Find all MSIL2A SAFE files in /S2_MSI
	safeL2Afull = '/S2_MSI'
	template = request.args.get('template', 'S2*MSIL2A*.SAFE')
	app.logger.warning('publishS2All - find {} -name {}'.format(safeL2Afull,template))
	SAFEfiles = [os.path.join(dirpath, f)
		for dirpath, dirnames, files in os.walk("{0}".format(safeL2Afull))
		for f in fnmatch.filter(dirnames, template)]
	count = 0
	for safe in sorted(SAFEfiles):
		count += 1
		app.logger.warning('publishS2All {}/{} - {}'.format(count,len(SAFEfiles),safe))
		scene = {}
		scene['file'] = safe
		publishS2(scene)
	return jsonify(SAFEfiles)

###################################################
@app.route('/sen2corAll', methods=['GET','POST'])
def sen2corAll():
# Find all MSIL2A SAFE files in /S2_MSI
	safeL1Cfull = '/S2_MSI'
	template =  "S2*MSIL1C*.SAFE"
	template = request.args.get('template', 'S2*MSIL1C*.SAFE')
	app.logger.warning('sen2corAll - find {} -name {}'.format(safeL1Cfull,template))
	SAFEfiles = [os.path.join(dirpath, f)
		for dirpath, dirnames, files in os.walk("{0}".format(safeL1Cfull))
		for f in fnmatch.filter(dirnames, template)]
	count = {'sen2cor' : 0, 'publishS2' : 0}
	for safe in SAFEfiles:
		parts = os.path.basename(safe).split('_')
		date = parts[2][0:8]
		tile = parts[5]
		identifier = parts[0]+'_'+parts[1]+'_'+date+'_'+parts[3]+'_'+parts[4]+'_'+tile
		app.logger.warning('sen2corAll - {}'.format(safe))
		activity = {}
		activity['file'] = safe
		activity['sceneid'] = identifier
		activity['id'] = None
		safeL2Afull = activity['file'].replace('MSIL1C','MSIL2A')
		if not os.path.exists(safeL2Afull):
			activity['app'] = 'sen2cor'
			activity['priority'] = 2
			count['sen2cor'] += 1
		else:
			activity['app'] = 'publishS2'
			activity['priority'] = 3
			count['publishS2'] += 1
		activity['status'] = 'NOTDONE'
		activity['message'] = ''
		activity['retcode'] = 0
		app.logger.warning('sen2corAll - activity {}'.format(activity))
		do_upsert('activities',activity,['id','status','link','start','end','elapsed','retcode','message'])
	start()
	return jsonify(count)


###################################################
@app.route('/upS2', methods=['GET','POST'])
def upS2():
# Read access keys
	if not os.path.exists("accessKeys.csv"):
		return 'No accessKeys.csv'
	fh = open('accessKeys.csv','r')
	line = fh.readline()
	line = fh.readline()
	line = line.strip()
	cc = line.split(",")

# Create an S3 client
	s3 = boto3.client('s3', aws_access_key_id=cc[0],aws_secret_access_key=cc[1])

# Call S3 to list current buckets
	response = s3.list_buckets()

# Get a list of all bucket names from the response
	buckets = [bucket['Name'] for bucket in response['Buckets']]
	bucket_name = None
	for bucket in buckets:
		if bucket.find('datastorm-repository') == 0:
			bucket_name = bucket
	
	if bucket_name is None:
		return 'No datastorm-repository'
	
	basedir = '/S2_MSI/2*'
	datedirs = sorted(glob.glob(basedir))
	for dir in datedirs:
		safes = glob.glob(dir+'/S2*_MSIL2A_*.SAFE')
		for safe in safes:
			published = safe+'/PUBLISHED/'
			prefix = 'Archive' + safe + '/'
			logging.warning('upS2 S3 prefix {} '.format(prefix))
			s3tiffs = []
			result = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
			if 'Contents' in result:
				for obj in result['Contents']:
					logging.warning('upS2 S3 tiff {} '.format(obj.get('Key')))
					s3tiffs.append(os.path.basename(obj.get('Key')))
			tiffs = glob.glob(published+'*.tif')
			for tiff in tiffs[:1]:
				if os.path.basename(tiff) in s3tiffs:
					logging.warning('upS2 {} already in S3'.format(os.path.basename(tiff)))
					continue
				mykey = 'Archive'+tiff
				mykey = mykey.replace('/PUBLISHED', '')
				logging.warning('upS2 tiff {} mykey {}'.format(tiff,mykey))
				with open(tiff, 'rb') as data:
					s3.upload_fileobj(data, bucket_name, mykey)

	return jsonify(bucket_name)
	bucket = s3.Bucket(bucket_name)
	upgrade_path = 'Archive/S2_MSI/'
	key = bucket.get_key(upgrade_path)
	logging.warning('upS2 key {} upgrade_path {}'.format(key,upgrade_path))
	if key is None:
		key = bucket.new_key(upgrade_path)
		logging.warning('upS2 new_key {}'.format(key))

# Print out the bucket list
	return jsonify(bucket_name)

###################################################
@app.route('/checkS3', methods=['GET','POST'])
def checkS3():
	global S3Client,bucket_name

	getS3Client()

# Call S3 to list current buckets
	response = S3Client.list_buckets()

# Get a list of all bucket names from the response
	buckets = [bucket['Name'] for bucket in response['Buckets']]
	bucket_name = None
	for bucket in buckets:
		if bucket.find('datastorm-archive') == 0:
			bucket_name = bucket
	
	if bucket_name is None:
		return 'No datastorm-archive'

	prefix = 'S2_MSI/'
	logging.warning('checkS3 S3 prefix {} '.format(prefix))
	s3tiffs = {}
	result = get_all_s3_keys(bucket_name,prefix)
	for tiff in result:
		parts = tiff.split('/')
		dataset = parts[0]
		if dataset not in s3tiffs:
			s3tiffs[dataset] = {}
		date = parts[1]
		if date not in s3tiffs[dataset]:
			s3tiffs[dataset][date] = {}
			logging.warning('checkS3 date {} '.format(date))
		sceneid = parts[2]
		if sceneid not in s3tiffs[dataset][date]:
			s3tiffs[dataset][date][sceneid] = 0
		s3tiffs[dataset][date][sceneid] += 1
		#logging.warning('checkS3 S3 tiff {} '.format(s3tiffs))

	for dataset in s3tiffs:
		for date in s3tiffs[dataset]:
			s3tiffs[dataset][date]['total']=len(s3tiffs[dataset][date])
	return jsonify(s3tiffs)

def get_all_s3_keys(bucket,prefix):
	global S3Client
	"""Get a list of all keys in an S3 bucket."""
	keys = []

	kwargs = {'Bucket': bucket,'Prefix': prefix}
	while True:
		resp = S3Client.list_objects_v2(**kwargs)
		for obj in resp['Contents']:
			keys.append(obj['Key'])

		try:
			kwargs['ContinuationToken'] = resp['NextContinuationToken']
		except KeyError:
			break

	return keys


###################################################
def getS3Client():
	global S3Client,bucket_name
# Read access keys
	if S3Client is None:
		if not os.path.exists("accessKeys.csv"):
			return 'No accessKeys.csv'
		fh = open('accessKeys.csv','r')
		line = fh.readline()
		line = fh.readline()
		line = line.strip()
		cc = line.split(",")

	# Create an S3 client
		S3Client = boto3.client('s3', aws_access_key_id=cc[0],aws_secret_access_key=cc[1])
		bucket_name = 'bdc-archive'

###################################################
@app.route('/uploadS2All', methods=['GET','POST'])
def uploadS2All():
	global S3Client,bucket_name,PAUSE
	bandmap = { \
"B01":"coastal", \
"B02":"blue", \
"B03":"green", \
"B04":"red", \
"B05":"redge1", \
"B06":"redge2", \
"B07":"redge3", \
"B08":"bnir", \
"B8A":"nir", \
"B09":"wvap", \
"B10":"cirrus", \
"B11":"swir1", \
"B12":"swir2", \
"EVI":"evi", \
"NDVI":"ndvi", \
"SCL":"quality" \
}
	PAUSE = False
	getS3Client()

# Call S3 to list current buckets
	response = S3Client.list_buckets()

# Get a list of all bucket names from the response
	buckets = [bucket['Name'] for bucket in response['Buckets']]
	if bucket_name not in buckets:
		return 'No {} in S3'.format(bucket_name)

# Find all MSIL2A SAFE files in /S2_MSI
	years = []
	year = request.args.get('year', None)
	if year is None:
		yearsf = glob.glob('/S2_MSI/*')
		for year in yearsf: years.append(os.path.basename(year))
	else:
		years.append(year)

# Connect to db and delete all data about this scene
	connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('CATALOG_USER'),
											  os.environ.get('CATALOG_PASS'),
											  os.environ.get('CATALOG_HOST'),
											  'catalogo')
	engine = sqlalchemy.create_engine(connection)
	for year in sorted(years):
		if PAUSE: return jsonify('PAUSE in - {}'.format(year))
		if year < '2017-08': continue
		SAFEfiles = glob.glob('/S2_MSI/{}/{}'.format(year,'S2*MSIL2A*.SAFE'))
		app.logger.warning('uploadS2All {} - SAFEfiles {}'.format('/S2_MSI/{}/{}'.format(year,'S2*MSIL2A*.SAFE'),len(SAFEfiles)))
		count = 0
		for safe in sorted(SAFEfiles):
			count += 1
			identifier = os.path.basename(safe).split('.')[0]
			app.logger.warning('uploadS2All {}/{} - {}'.format(count,len(SAFEfiles),safe))
			if PAUSE: return jsonify('PAUSE in {}/{} - {}'.format(count,len(SAFEfiles),safe))
# dst is /S2SR/yyyy-mm/safe
			dst = safe.replace('S2_MSI','S2SR')
# Check if files are already in dst
			newprefix = dst[1:]
			result = S3Client.list_objects_v2(Bucket=bucket_name, Prefix=newprefix)
			logging.warning('uploadS2All checking if S3 exists {}'.format(newprefix))
			if 'Contents' in result and len(result['Contents']) == 16: continue
# src is /S2_MSI/yyyy-mm/safe/PUBLISHED
			src = safe+'/PUBLISHED'
			dir = dst.split('/')
# dstforcopytree is /S2SR/yyyy-mm
			dstforcopytree = '/'.join(dir[:-1])

# Check if all 15 tiff files have already been published in the new structure
			newtiffs = []
			if os.path.exists(dst):
				newtiffs = glob.glob(dst+'/*.tif')
			if len(newtiffs) != 15:

# If files have not been published in the old structure, publish them in the new structure
				srcexists = os.path.exists(src)
				tiffs = []
				if srcexists:
					tiffs = glob.glob(src+'/*.tif')
				logging.warning('uploadS2All checking tiff in src {} - '.format(len(tiffs)))
				if not srcexists or len(tiffs) != 15:
					activity = {}
					activity['file'] = safe.replace('MSIL2A','MSIL1C')
					activity['sceneid'] = identifier.replace('MSIL2A','MSIL1C')
					activity['id'] = None
					activity['app'] = 'publishS2'
					activity['priority'] = 2
					activity['status'] = 'NOTDONE'
					activity['satellite'] = 'S2'
					activity['message'] = ''
					activity['retcode'] = 0
					logging.warning('uploadS2All - activity {}'.format(activity))
					do_upsert('activities',activity,['id','status','link','start','end','elapsed','retcode','message'])
					continue
					#publishS2(activity)
				else:
# Copy from the old to new structure
					tiffs = []
					if not os.path.exists(dstforcopytree):
						os.makedirs(dstforcopytree)
					step_start = time.time()
					copytree(src, dst)
					elapsedtime = time.time() - step_start
					ela = str(datetime.timedelta(seconds=elapsedtime))
					logging.warning('uploadS2All copytree {} to {} in {}'.format(src,dstforcopytree,ela))

# Check if all files are already in dst
			tiffs = glob.glob(dst+'/*.tif')
			if False and len(tiffs) != 15:
				activity = {}
				activity['file'] = safe.replace('MSIL2A','MSIL1C')
				activity['sceneid'] = identifier.replace('MSIL2A','MSIL1C')
				activity['id'] = None
				activity['app'] = 'sen2cor'
				activity['priority'] = 0
				activity['status'] = 'FORLATER'
				activity['satellite'] = 'S2'
				activity['message'] = ''
				activity['retcode'] = 0
				logging.warning('uploadS2All - activity {}'.format(activity))
				do_upsert('activities',activity,['id','status','link','start','end','elapsed','retcode','message'])
				continue
				#return jsonify('uploadS2All error processing - {} only {} tiffs\n'.format(safe,len(tiffs)))
			png = glob.glob(dst+'/*.png')
			if len(png) != 1: return jsonify('uploadS2All error processing - {} no png\n'.format(safe))

# Check if files are already in S3 S2SR
			newprefix = dst[1:]
			result = S3Client.list_objects_v2(Bucket=bucket_name, Prefix=newprefix)
			logging.warning('uploadS2All checking if S3 exists {}'.format(newprefix))
			if 'Contents' not in result or len(result['Contents']) < 15:
# If files are not in S3 S2SR, chech if they are in S2_MSI and copy them to S2SR
				s3objs = []
				step_start = time.time()
				oldprefix = safe[1:].replace('S2_MSI','S2_MSI') + '/PUBLISHED/'
				oldresult = S3Client.list_objects_v2(Bucket=bucket_name, Prefix=oldprefix)
				if 'Contents' in oldresult:
					for obj in oldresult['Contents']:
						logging.warning('uploadS2All S3 {} '.format(obj.get('Key')))
						s3objs.append(os.path.basename(obj.get('Key')))
						copy_source = {'Bucket': bucket_name, 'Key': obj.get('Key')}
						dest_object_name = obj.get('Key')
						dest_object_name = dest_object_name.replace('PUBLISHED/','')
						dest_object_name = dest_object_name.replace('S2_MSI','S2SR')
						logging.warning('uploadS2All S3 {} to {}'.format(obj.get('Key'),dest_object_name))
						try:
							S3Client.head_object(Bucket=bucket_name, Key=dest_object_name)
							logging.warning('uploadS2All S3 exists {}'.format(dest_object_name))
						except:
							S3Client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=dest_object_name)
				elapsedtime = time.time() - step_start
				ela = str(datetime.timedelta(seconds=elapsedtime))
				app.logger.warning('uploadS2All copy_object in {}'.format(ela))
				logging.warning('uploadS2All files transferred from S2_MSI to S2SR {}'.format(len(s3objs)))
			tiffs = glob.glob(dst+'/*.tif')
			for tiff in tiffs:
				band = os.path.basename(tiff).split('_')[-1].split('.')[0]
				sband = bandmap[band]
				sql = "UPDATE Product SET Filename ='{}' WHERE SceneId = '{}' AND Band='{}'".format(tiff,identifier,sband)
				logging.warning('uploadS2All sql {}'.format(sql))
				engine.execute(sql)
			png = glob.glob(dst+'/*.png')
			sql = "UPDATE Qlook SET QLfilename='{}' WHERE SceneId = '{}'".format(png[0],identifier)
			app.logger.warning('uploadS2All sql {}'.format(sql))
			engine.execute(sql)
# If all 16 files are already in S3, no need to upload them
			result = S3Client.list_objects_v2(Bucket=bucket_name, Prefix=newprefix)
			logging.warning('uploadS2All checking if S3 exists {}'.format(newprefix))
			if 'Contents' in result and len(result['Contents']) == 16: continue
			if dst == '': return jsonify('uploadS2All error processing - {}\n'.format(safe))

			activity = {}
			activity['file'] = dst
			activity['sceneid'] = identifier
			activity['id'] = None
			activity['app'] = 'uploadS2'
			activity['priority'] = 2
			activity['status'] = 'NOTDONE'
			activity['message'] = ''
			activity['retcode'] = 0
			logging.warning('uploadS2All - activity {}'.format(activity))
			do_upsert('activities',activity,['id','status','link','start','end','elapsed','retcode','message'])
			start()
	engine.dispose()
	return jsonify('SAFEs - {}\n'.format(len(SAFEfiles)))

###################################################
def uploadS2(scene):
	global S3Client,bucket_name
	getS3Client()

	safe = scene['file'].replace('MSIL1C','MSIL2A')
	published = safe+'/PUBLISHED/'
	prefix = safe[1:] + '/'
	prefix = prefix.replace('S2_MSI','S2SR')
	logging.warning('uploadS2 S3 prefix {} '.format(prefix))
	s3tiffs = []
	result = S3Client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
	if 'Contents' in result:
		for obj in result['Contents']:
			logging.warning('upS2 S3 tiff {} '.format(obj.get('Key')))
			s3tiffs.append(os.path.basename(obj.get('Key')))
	tiffs = glob.glob(published+'*.tif')
	count = 0
	for tiff in tiffs:
		count += 1
		logging.warning('uploadS2 {}/{} - {}'.format(count,len(tiffs),tiff))
		if os.path.basename(tiff) in s3tiffs:
			logging.warning('uploadS2 {} already in S3'.format(os.path.basename(tiff)))
			continue
		mykey = tiff[1:]
		#mykey = mykey.replace('/PUBLISHED', '')
		#logging.warning('uploadS2 tiff {} mykey {}'.format(tiff,mykey))
		
		try:
			tc = boto3.s3.transfer.TransferConfig(use_threads=True,max_concurrency=ACTIVITIES['uploadS2']['maximum'])
			t = boto3.s3.transfer.S3Transfer( client=S3Client, config=tc )
			t.upload_file( tiff, bucket_name, mykey, extra_args={'ACL': 'public-read'})
		except Exception as e:
			logging.warning('uploadS2 error {}'.format(e))
			return 1
		
		
		"""
		with open(tiff, 'rb') as data:
			try:
				S3Client.upload_fileobj(data, bucket_name, mykey)
			#except botocore.exceptions.BotoCoreError as err:
			except Exception as e:
				logging.warning('uploadS2 error {}'.format(e))
				return 1
		"""
	return 0

###################################################
@app.route('/getS2', methods=['GET','POST'])
def getS2():

# Get the requested period to be processed
	rstart = request.args.get('start', '2018-12-13')
	rend   = request.args.get('end', '2018-12-31')
	cloud = float(request.args.get('cloud', CLOUD_DEFAULT))
	limit = request.args.get('limit', 200)
	mgrs = request.args.get('mgrs', None)
	action = request.args.get('action', 'search')

# Connect to db datastorm
	connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('CATALOG_USER'),
											  os.environ.get('CATALOG_PASS'),
											  os.environ.get('CATALOG_HOST'),
											  'datastorm')

	if mgrs is None:
		sql = "SELECT * FROM wrs WHERE name = 'S2'"
	else:
		sql = "SELECT * FROM wrs WHERE name = 'S2' AND tileid LIKE '{}%%'".format(mgrs)
	engine = sqlalchemy.create_engine(connection)
	wrss = engine.execute(sql)
	wrss = wrss.fetchall()
	logging.warning('getS2 - '+sql+' - Rows: {}'.format(wrss))
	engine.dispose()
	wrss = [dict(wrs) for wrs in wrss]
	scenesperpr = {}
	basedir = '/S2_MSI/2*'
	datedirs = sorted(glob.glob(basedir))
	for wrs in wrss:
		w = wrs['lonmin']
		e = wrs['lonmax']
		s = wrs['latmin']
		n = wrs['latmax']
		w = float(w)
		e = float(e)
		s = float(s)
		n = float(n)
		clon = (w+e)/2
		clat = (n+s)/2
		pathrow = wrs['tileid']
		tileExist = False
		for dir in datedirs:
			for type in ['MSIL2A','MSIL1C']:
				files = glob.glob(dir+'/S2*_{}_*{}*.SAFE'.format(type,pathrow))
				if len(files) > 0:
					tileExist = True
		app.logger.warning('getS2 - tileid {} tileExist - {}'.format(pathrow,tileExist))
		if 	tileExist: continue

		footprintWkt = wrs['geom']
		footprintPoly = ogr.CreateGeometryFromWkt(footprintWkt)
		if pathrow not in scenesperpr:
			scenesperpr[pathrow] = {'footprintPoly' : footprintPoly, 'scenes': {'MSIL1C':{},'MSIL2A':{}} }
		scenes = openSearchS2SAFE(clon,clat,clon,clat,rstart,rend,cloud,limit)
		for sceneid in scenes:
			scene = scenes[sceneid]
			type = scene['type']
			scenesperpr[pathrow]['scenes'][type].update({sceneid:scene})
	scenes = {}
	count = 0
	for pathrow in scenesperpr:
		best = {'sceneid': None,'cloud': 0.,'arearatio': 0}
		scenes[pathrow] = {}
		for type in ['MSIL2A','MSIL1C']:
			for sceneid in scenesperpr[pathrow]['scenes'][type]:
				size = scenesperpr[pathrow]['scenes'][type][sceneid]['size']
				poly = ogr.CreateGeometryFromWkt(scenesperpr[pathrow]['scenes'][type][sceneid]['footprint'])
				footprintPoly = scenesperpr[pathrow]['footprintPoly']
				intersection = footprintPoly.Intersection(poly)
				arearatio = round(100*intersection.GetArea()/footprintPoly.GetArea(),1)
				cloud = float(scenesperpr[pathrow]['scenes'][type][sceneid]['cloud'])
				logging.warning('getS2 - pathrow: {} type {} sceneid {} cloud {} arearatio {} size {} {} {}'.format(pathrow,type,sceneid,cloud,arearatio,size,poly.GetArea(),footprintPoly.GetArea()))
				if best['sceneid'] is None:
					best['sceneid'] = sceneid
					best['cloud'] = cloud
					best['arearatio'] = arearatio
					best['type'] = type
				else:
					if best['cloud'] > cloud and best['arearatio'] <= arearatio:
						best['sceneid'] = sceneid
						best['cloud'] = cloud
						best['arearatio'] = arearatio
						best['type'] = type
		if best['sceneid'] is None: continue
		scenes[pathrow][best['sceneid']] = {'arearatio':best['arearatio'],'cloud':best['cloud'],'type':best['type'] }
		scene = scenesperpr[pathrow]['scenes'][best['type']][best['sceneid']]
		logging.warning('getS2 - pathrow: {} bestsceneid {}'.format(pathrow,best['sceneid']))
		activity = {}
		activity['id'] = -1
		activity['app'] = 'downloadS2'
		activity['sceneid'] = scene['sceneid']
		activity['satellite'] = 'S2'
		activity['priority'] = 1
		activity['status'] = 'NOTDONE'
		activity['link'] = scene['link'].replace("'","''")
		count += 1
		if action != 'search':
			do_upsert('activities',activity,['id','status','link'])
				
	if action != 'search':
		start()
	scenes['scenes']=count
	return jsonify(scenes)

###################################################
@app.route('/getS2old', methods=['GET','POST'])
def getS2old():

# Get the requested period to be processed
	rstart = request.args.get('start', '2018-12-13')
	rend   = request.args.get('end', '2018-12-31')
	cloud = float(request.args.get('cloud', CLOUD_DEFAULT))
	limit = request.args.get('limit', 200)
	mgrs = request.args.get('mgrs', None)
	action = request.args.get('action', 'search')

# Connect to db datastorm
	connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('CATALOG_USER'),
											  os.environ.get('CATALOG_PASS'),
											  os.environ.get('CATALOG_HOST'),
											  'datastorm')

	if mgrs is None:
		sql = "SELECT * FROM wrs WHERE name = 'S2'"
	else:
		sql = "SELECT * FROM wrs WHERE name = 'S2' AND tileid LIKE '{}%%'".format(mgrs)
	engine = sqlalchemy.create_engine(connection)
	wrss = engine.execute(sql)
	wrss = wrss.fetchall()
	logging.warning('getS2 - '+sql+' - Rows: {}'.format(wrss))
	engine.dispose()
	wrss = [dict(wrs) for wrs in wrss]
	scenesperpr = {}
	for wrs in wrss:
		w = wrs['lonmin']
		e = wrs['lonmax']
		s = wrs['latmin']
		n = wrs['latmax']
		w = float(w)
		e = float(e)
		s = float(s)
		n = float(n)
		clon = (w+e)/2
		clat = (n+s)/2
		pathrow = wrs['tileid']
		footprintWkt = wrs['geom']
		footprintPoly = ogr.CreateGeometryFromWkt(footprintWkt)
		if pathrow not in scenesperpr:
			scenesperpr[pathrow] = {'footprintPoly' : footprintPoly, 'scenes': {'MSIL1C':{},'MSIL2A':{}} }
		scenes = openSearchS2SAFE(clon,clat,clon,clat,rstart,rend,cloud,limit)
		for sceneid in scenes:
			scene = scenes[sceneid]
			type = scene['type']
			scenesperpr[pathrow]['scenes'][type].update({sceneid:scene})
		#scenesperpr[pathrow]['scenes']['S2MSI1C'].update(openSearchS2SAFE(clon,clat,clon,clat,rstart,rend,cloud,limit,'S2MSI1C'))
		#scenesperpr[pathrow]['scenes']['S2MSI2A'].update(openSearchS2SAFE(clon,clat,clon,clat,rstart,rend,cloud,limit,'S2MSI2A'))
	scenes = {}
	for pathrow in scenesperpr:
		scenes[pathrow] = {}
		for type in ['MSIL2A','MSIL1C']:
			scenes[pathrow][type] = {}
			for sceneid in scenesperpr[pathrow]['scenes'][type]:
				tileid = sceneid.split('_')[5]
				date = sceneid.split('_')[2][:6]
				yyyymm = date[:4]+'-'+date[4:]
				files = glob.glob('/S2_MSI/'+yyyymm+'/S2*_{}_*{}*.SAFE'.format(type,tileid))
				scenesperpr[pathrow]['scenes'][type][sceneid]['files'] = files
				poly = ogr.CreateGeometryFromWkt(scenesperpr[pathrow]['scenes'][type][sceneid]['footprint'])
				intersection = scenesperpr[pathrow]['footprintPoly'].Intersection(poly)
				arearatio = round(100*intersection.GetArea()/footprintPoly.GetArea(),1)
				scenes[pathrow][type][sceneid] = {'files':len(files),'arearatio':arearatio,'cloud':scenesperpr[pathrow]['scenes'][type][sceneid]['cloud'] }
				logging.warning('getS2 - pathrow: {} sceneid {} tileid {} {} files {} arearatio {}'.format(pathrow,sceneid,tileid,yyyymm,files,arearatio))
	if action == 'search':
		return jsonify(scenes)
	for pathrow in scenesperpr:
		type = 'MSIL2A'
		l2Aexists = False
		logging.warning('getS2 - checking l2A pathrow: {} scenes2A {}'.format(pathrow,len(scenesperpr[pathrow]['scenes'][type])))
		if len(scenesperpr[pathrow]['scenes'][type]) > 0:
			for sceneid in scenesperpr[pathrow]['scenes'][type]:
				logging.warning('getS2 - checking l2A pathrow: {} sceneid {} files {}'.format(pathrow,sceneid,len(scenesperpr[pathrow]['scenes'][type][sceneid]['files'])))
				if len(scenesperpr[pathrow]['scenes'][type][sceneid]['files']) > 0:
					l2Aexists = True
					logging.warning('getS2 - pathrow: {} sceneid {} l2Aexists'.format(pathrow,sceneid))
					continue
		if l2Aexists:
			continue
		type = 'MSIL1C'
		l1Cexists = False
		bestl1C = {'sceneid': None,'cloud': 0}
		logging.warning('getS2 - pathrow: {} checking bestl1C {}'.format(pathrow,bestl1C))
		if len(scenesperpr[pathrow]['scenes'][type]) > 0:
			for sceneid in scenesperpr[pathrow]['scenes'][type]:
				if len(scenesperpr[pathrow]['scenes'][type][sceneid]['files']) > 0:
					logging.warning('getS2 - pathrow: {} sceneid {} l1Cexists {} files'.format(pathrow,sceneid,scenesperpr[pathrow]['scenes'][type][sceneid]['files']))
					l1Cexists = True
				if bestl1C['sceneid'] is None:
					bestl1C['sceneid'] = sceneid
					bestl1C['cloud'] = scenesperpr[pathrow]['scenes'][type][sceneid]['cloud']
				else:
					if bestl1C['cloud'] > scenesperpr[pathrow]['scenes'][type][sceneid]['cloud']:
						bestl1C['sceneid'] = sceneid
						bestl1C['cloud'] = scenesperpr[pathrow]['scenes'][type][sceneid]['cloud']
				logging.warning('getS2 - pathrow: {} checking sceneid {} cloud {} bestl1C {}'.format(pathrow,sceneid,scenesperpr[pathrow]['scenes'][type][sceneid]['cloud'],bestl1C))

			logging.warning('getS2 - pathrow: {} checked bestl1C {}'.format(pathrow,bestl1C))
			scene = scenesperpr[pathrow]['scenes'][type][bestl1C['sceneid']]
			logging.warning('getS2 - pathrow: {} bestsceneid {}'.format(pathrow,bestl1C['sceneid']))
			activity = {}
			activity['id'] = -1
			activity['app'] = 'downloadS2'
			activity['sceneid'] = scene['sceneid']
			activity['satellite'] = 'S2'
			activity['priority'] = 1
			activity['status'] = 'NOTDONE'
			activity['link'] = scene['link'].replace("'","''")
			sql = "SELECT * FROM activities WHERE status = 'DONE' AND app = 'downloadS2'  AND sceneid = '{}'".format(scene['sceneid'])
			activities = do_query(sql)
			if len(activities) > 0:
				activity = activities[0]
				app.logger.warning('getS2 - activity already done {}'.format(activity))
				manage(activity)
			else:
				sql = "SELECT * FROM activities WHERE (status = 'DOING' OR status = 'NOTDONE') AND app = 'downloadS2'  AND sceneid = '{}'".format(scene['sceneid'])
				activities = do_query(sql)
				if len(activities) == 0:
					activity['status'] = 'NOTDONE'
					app.logger.warning('getS2 - activity new {}'.format(activity))
					do_upsert('activities',activity,['id','status','link'])
				else:
					app.logger.warning('getS2 - already queued {}'.format(activity))
				
	#start()
	scenes['scenes']=len(scenes)
	return jsonify(scenes)



###################################################
def getSESSION():
	global SESSION
# USGS access
	if SESSION is None:
	# Read access keys
		if not os.path.exists("secrets_USGS.csv"):
			return 'No secrets_USGS.csv'
		fh = open('secrets_USGS.csv','r')
		line = fh.readline()
		line = fh.readline()
		line = line.strip()
		cc = line.split(",")

		usgs_user = str(cc[0])
		usgs_pass = str(cc[1])
		url_login = 'https://ers.cr.usgs.gov/login/'
		SESSION = requests.Session()
		login_html = SESSION.get(url_login)

		html = bs4.BeautifulSoup(login_html.content, "html.parser")

		__ncforminfo = html.find("input", {"name": "__ncforminfo"}).attrs.get("value")
		csrf_token = html.find("input", {"id": "csrf_token"}).attrs.get("value")

		auth = {"username": usgs_user, "password": usgs_pass, "csrf_token": csrf_token, "__ncforminfo": __ncforminfo}

		SESSION.post(url_login, data=auth, allow_redirects=False)


###################################################
@app.route('/pause', methods=['GET'])
def pause():
	sql = "UPDATE activities SET status='SUSPEND' WHERE status = 'NOTDONE'"
	do_command(sql)
	msg = 'sql - {}\n'.format(sql)

	return msg


###################################################
@app.route('/radcor', methods=['GET'])
def radcor():
	args = {}
	for key in request.args:
		args[key] = request.args.get(key)

	action = args['action'] if 'action' in args else 'search'

# Create a list to store all queries
	activitylist = []

# Get the requested period to be processed
	dstart = args['start'] if 'start' in args else None
	dend = args['end'] if 'end' in args else None

	if 'w' in args and 'n' in args and 'e' in args and 's' in args:
		activity = {}
		activity['limit'] = args['limit'] if 'limit' in args else 299
		activity['cloud'] = args['cloud'] if 'cloud' in args else CLOUD_DEFAULT
		activity['tileid'] = 'notile'
		activity['satsen'] = args['satsen'].split(',') if 'satsen' in args else ['S2']
		activity['w'] = args['w']
		activity['e'] = args['e']
		activity['s'] = args['s']
		activity['n'] = args['n']
		activity['start'] = dstart
		activity['end'] = dend
		activity['action'] = action
		activitylist.append(activity)

# Retrieve datacube info
	elif 'datacube' in args:
# Connect to db datastorm
		connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('CATALOG_USER'),
												  os.environ.get('CATALOG_PASS'),
												  os.environ.get('CATALOG_HOST'),
												  'datastorm')

# Get information from datacube
		sql = "SELECT * FROM datacubes WHERE datacube = '{}'".format(args['datacube'])
		engine = sqlalchemy.create_engine(connection)
		result = engine.execute(sql)
		datacube = result.fetchone()
		if datacube is None:
			engine.dispose()
			app.logger.warning('radcor - datacube {} not yet created'.format(args['datacube']))
			return jsonify({'code': 301, 'message': 'radcor - datacube {} not yet created'.format(args['datacube'])})

		satsen = args['satsen'].split(',') if 'satsen' in args else []
		if len(satsen) == 0:
			satsen = datacube['satsen'].split(',')

# Decode the requested tiles to be processed
		tileidlist =  decodePathRow(args['tileid']) if 'tileid' in args else []

# Get the time line for the datacube
		periodlist = decodePeriods(datacube['tschema'],datacube['start'],datacube['end'],datacube['step'])

# Decode the requested tiles to be processed if specific tiles were notin requests
		if len(tileidlist) == 0:
			sql = "SELECT DISTINCT tileid FROM mosaics WHERE datacube = '{}'".format(args['datacube'])
			results = engine.execute(sql)
			results = results.fetchall()
			app.logger.warning('radcor - sql {} results {} '.format(sql,results))
			for result in results:
				tileidlist.append(result['tileid'])

# Retrieve the mosaics to be processed
		for tileid in tileidlist:
			sql = "SELECT * FROM wrs WHERE name = '{}' AND tileid = '{}'".format(datacube['wrs'],tileid)
			results = engine.execute(sql)
			result = results.fetchone()
			app.logger.warning('radcor - sql {} results {} '.format(sql,result))
			if result is None:
				continue
			for datekey in sorted(periodlist):
				requestedperiod = periodlist[datekey]
				for periodkey in requestedperiod:
					(basedate,startdate,enddate) = periodkey.split('_')
					app.logger.warning('radcor - startdate {} dstart {} '.format(startdate,dstart))
					if dstart is not None and startdate < dstart : continue
					if dend is not None and enddate > dend : continue
					activity = {}
					activity['limit'] = args['limit'] if 'limit' in args else 299
					activity['cloud'] = args['cloud'] if 'cloud' in args else CLOUD_DEFAULT
					activity['tileid'] = tileid
					activity['satsen'] = satsen
					activity['w'] = result['lonmin']
					activity['e'] = result['lonmax']
					activity['s'] = result['latmin']
					activity['n'] = result['latmax']
					activity['start'] = startdate
					activity['end'] = enddate
					activity['action'] = action
					activitylist.append(activity)
	else:
		return jsonify({'code': 300, 'message': 'Datacube or Bounding Box must be given'})

	scenes = {}
	total = {}
	for activity in activitylist:
		result = doradcor(activity)
		if 'LC8' in activity['satsen'] or 'LC8SR' in activity['satsen']:
			result = filter(result,tags=['cloud','date','status'])
		else:
			result = filter(result)
		scenes['{}-{}-{}'.format(activity['tileid'],activity['start'],activity['end'])] = result
		total['{}-{}-{}'.format(activity['tileid'],activity['start'],activity['end'])]= len(result)
	scenes['Results'] = total
	return jsonify(scenes)
	return jsonify({'code': 200, 'message': '{}'.format(scenes)})

###################################################
def doradcor(args):

	app.logger.warning('doradcor - args {}'.format(args))
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
		result = developmentSeed_sat_api(w,n,e,s,rstart,rend,cloud,limit)
		scenes.update(result)
		for id in result:
			scene = result[id]
			sceneid = scene['sceneid']
# Check if this scene is already in Repository
			cc = sceneid.split('_')
			yyyymm = cc[3][:4]+'-'+cc[3][4:6]
			tileid = cc[2]
# Find LC08_L1TP_218069_20180706_20180717_01_T1.png
			LC8SRfull = '/Repository/Archive/LC8SR/{}/{}/'.format(yyyymm,tileid)
			template =  LC8SRfull+'{}.png'.format(sceneid)
			app.logger.warning('doradcor - find {}'.format(template))
			LC8SRfiles = glob.glob(template)
			if len(LC8SRfiles) > 0: 
				logging.warning('radcor - {} already done'.format(sceneid))
				scene['status'] = 'DONE'
				continue
			scene['status'] = 'NOTDONE'
			activity = {}
			activity['app'] = 'downloadLC8'
			activity['status'] = 'NOTDONE'
			activity['sceneid'] = scene['sceneid']
			activity['satellite'] = 'LC8'
			activity['priority'] = 1
			activity['link'] = scene['link'].replace("'","''")
			app.logger.warning('radcor - activity {}'.format(activity))
			if action != 'search':
				do_upsert('activities',activity,['id','status','link'])
	if 'S2' in sat or 'S2SR' in sat:
		result = openSearchS2SAFE(w,n,e,s,rstart,rend,cloud,limit)
		scenes.update(result)
		for id in result:
			scene = result[id]
			sceneid = scene['sceneid']
# Check if this scene is already in Repository as Level 2A
			cc = sceneid.split('_')
			yyyymm = cc[2][:4]+'-'+cc[2][4:6]
# Output product dir 
			productdir = '/S2_MSI/{}/'.format(yyyymm)
# Check if an equivalent sceneid has already been downloaded
			date = cc[2]
			tile = cc[5]
			files = glob.glob(productdir+'S*MSIL2A_{}*{}*.SAFE'.format(date,tile))
			logging.warning('radcor - {} {}'.format(productdir+'S*MSIL2A_{}*{}*.SAFE'.format(date,tile),files))
			if len(files) > 0:
				logging.warning('radcor - {} already done'.format(sceneid))
				scene['status'] = 'DONE'
				continue
			
			safeL2Afull = productdir+sceneid.replace('MSIL1C','MSIL2A')+'.SAFE'
			if os.path.exists(safeL2Afull):
				app.logger.warning('radcor - scene exists {}'.format(safeL2Afull))
				scene['status'] = 'DONE'
				continue
				
			scene['status'] = 'NOTDONE'
			activity = {}
			activity['app'] = 'downloadS2'
			activity['sceneid'] = sceneid
			activity['satellite'] = 'S2'
			activity['priority'] = 1
			activity['link'] = scene['link'].replace("'","''")
			sql = "SELECT * FROM activities WHERE sceneid = '{}' AND (status = 'DONE' OR status = 'DOING')".format(scene['sceneid'])
			activities = do_query(sql)
			if len(activities) > 0:
				app.logger.warning('radcor - activity already done {}'.format(len(activities)))
				continue
			activity['status'] = 'NOTDONE'
			app.logger.warning('radcor - activity new {}'.format(activity))
			if action != 'search':
				do_upsert('activities',activity,['id','status','link'])
			scenes[id] = scene
	if action != 'search': start()
	return scenes

###################################################
def filter(scenes,tags=['cloud','date','status','size']):
	app.logger.warning('filter - scenes {}'.format(scenes))
	newscenes = {}
	for sceneid in scenes:
		scene = scenes[sceneid]
		newscenes[sceneid] = {}
		for tag in tags:
			newscenes[sceneid][tag] = scene[tag]
	return newscenes

###################################################
@app.route('/consultscenes', methods=['GET','POST'])
# Return Scene IDs as JSON given a time interval and a set of sensors
def consultscenes():
	args = {}
	for key in request.args:
		args[key] = request.args.get(key)
		
	# Get the requested period
	rstart = args['start'] if 'start' in args else None
	rend = args['end'] if 'end' in args else None
	# Get the requested satsens
	satsens = args['satsen'].split(',')

	# Connect to db datastorm
	connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('CATALOG_USER'),
												os.environ.get('CATALOG_PASS'),
												os.environ.get('CATALOG_HOST'),
												'catalogo')

	# Get information from datacube
	sql = "SELECT SceneId FROM `Scene` WHERE (Date >= '{}' AND Date <= '{}') AND (".format(rstart, rend)
	cont_satsen = 0 
	for satsen in satsens:
		sql += "Sensor = '{}'".format(satsen)
		cont_satsen += 1
		if (cont_satsen < len(satsens)):
			sql += " OR "
	sql += ')'
	app.logger.warning('ConsultScenes - {}'.format(sql))
	engine = sqlalchemy.create_engine(connection)
	results = engine.execute(sql)
	results = results.fetchall()
	app.logger.warning('results {}'.format(results))
	return jsonify({'result': [dict(row)['SceneId'] for row in results]})

###################################################
@app.route('/radcorold', methods=['GET','POST'])
def radcorold():
	global MAX_THREADS
	#redis.set('rc_lock',0)
	args = {}
	for key in request.args:
		args[key] = request.args.get(key)
	#return jsonify(args)

	sat = request.args.get('sat', 'S2')
# Get bbox
	w = request.args.get('w', -57.43)
	e = request.args.get('e', -54.3)
	s = request.args.get('s', -17.42)
	n = request.args.get('n', -14)
	w = float(w)
	e = float(e)
	s = float(s)
	n = float(n)
	
# Get the requested period to be processed
	rstart = request.args.get('start', '2019-02-01')
	rend   = request.args.get('end', None)
	cloud = float(request.args.get('cloud', CLOUD_DEFAULT))
	limit = request.args.get('limit', 200)

# Manage lock
	lock = getLock()
	#app.logger.warning('radcor - lock is: {}'.format(lock))

# Manage MAX_THREADS
	nt = request.args.get('nt', None)
	if MAX_THREADS is None:
		MAX_THREADS = int(os.environ.get('MAX_THREADS'))
		app.logger.warning('process - MAX_THREADS was None, now is: {} nt is: {}'.format(MAX_THREADS,nt))
	if nt is not None:
		MAX_THREADS = int(nt)
	app.logger.warning('radcor - MAX_THREADS is: {} nt is: {}'.format(MAX_THREADS,nt))

	scenes = {}
	if 'LC8' in sat:
		# result = developmentSeed(w,n,e,s,rstart,rend,cloud,limit)
		result = developmentSeed_sat_api(w,n,e,s,rstart,rend,cloud,limit)
		scenes.update(result)
		for id in result:
			scene = result[id]
			activity = {}
			activity['app'] = 'downloadLC8'
			activity['status'] = 'NOTDONE'
			activity['sceneid'] = scene['sceneid']
			activity['satellite'] = 'LC8'
			activity['priority'] = 1
			activity['link'] = scene['link'].replace("'","''")
			app.logger.warning('radcor - activity {}'.format(activity))
			do_upsert('activities',activity,['id','status','link'])
	if 'S2' in sat:
		result = openSearchS2SAFE(w,n,e,s,rstart,rend,cloud,limit)
		scenes.update(result)
		for id in result:
			scene = result[id]
			sceneid = scene['sceneid']
# Check if this scene is already in Repository as Level 2A
			cc = sceneid.split('_')
			yyyymm = cc[2][:4]+'-'+cc[2][4:6]
# Output product dir 
			productdir = '/S2_MSI/{}/'.format(yyyymm)
# Check if an equivalent sceneid has already been downloaded
			date = cc[2]
			tile = cc[5]
			files = glob.glob(productdir+'S*MSIL2A_{}*{}*.SAFE'.format(date,tile))
			logging.warning('radcor - {} {}'.format(productdir+'S*MSIL2A_{}*{}*.SAFE'.format(date,tile),files))
			if len(files) > 0:
				logging.warning('radcor - {} already done'.format(sceneid))
				continue
			
			safeL2Afull = productdir+sceneid.replace('MSIL1C','MSIL2A')+'.SAFE'
			if os.path.exists(safeL2Afull):
				app.logger.warning('radcor - scene exists {}'.format(safeL2Afull))
				continue
				
			activity = {}
			activity['app'] = 'downloadS2'
			activity['sceneid'] = sceneid
			activity['satellite'] = 'S2'
			activity['priority'] = 1
			activity['link'] = scene['link'].replace("'","''")
			"""
			sql = "SELECT * FROM activities WHERE status = 'DONE' AND app = 'downloadS2'  AND sceneid = '{}'".format(scene['sceneid'])
			activities = do_query(sql)
			if len(activities) > 0:
				activity = activities[0]
				app.logger.warning('radcor - activity already done {}'.format(activity))
				manage(activity)
			else:
				sql = "SELECT * FROM activities WHERE (status = 'DOING' OR status = 'NOTDONE') AND app = 'downloadS2'  AND sceneid = '{}'".format(scene['sceneid'])
				activities = do_query(sql)
				if len(activities) == 0:
					activity['status'] = 'NOTDONE'
					app.logger.warning('radcor - activity new {}'.format(activity))
					do_upsert('activities',activity,['id','status','link'])
					scenes.update(result)
				else:
					app.logger.warning('radcor - already queued {}'.format(activity))
			"""
			sql = "SELECT * FROM activities WHERE sceneid = '{}' AND status = 'DONE'".format(scene['sceneid'])
			activities = do_query(sql)
			if len(activities) > 0:
				app.logger.warning('radcor - activity already done {}'.format(len(activities)))
				continue
			activity['status'] = 'NOTDONE'
			app.logger.warning('radcor - activity new {}'.format(activity))
			do_upsert('activities',activity,['id','status','link'])
			scenes[id] = scene
	start()
	scenes['scenes']=len(scenes)
	return jsonify(scenes)

###################################################
@app.route('/search', methods=['GET','POST'])
def search():
	args = {}
	for key in request.args:
		args[key] = request.args.get(key)

	sat = request.args.get('sat', 'S2')
# Get bbox
	w = request.args.get('w', -57.43)
	e = request.args.get('e', -54.3)
	s = request.args.get('s', -17.42)
	n = request.args.get('n', -14)
	w = float(w)
	e = float(e)
	s = float(s)
	n = float(n)

# Get the requested period to be processed
	rstart = request.args.get('start', '2018-07-01')
	rend   = request.args.get('end', None)
	cloud = float(request.args.get('cloud', CLOUD_DEFAULT))
	limit = request.args.get('limit', 200)

	scenes = {}
	if 'LC8' in sat:
		# result = developmentSeed(w,n,e,s,rstart,rend,cloud,limit)
		result = developmentSeed_sat_api(w,n,e,s,rstart,rend,cloud,limit)
		scenes.update(result)
	if 'S2' in sat:
		result = openSearchS2SAFE(w,n,e,s,rstart,rend,cloud,limit)
		scenes.update(result)

	return jsonify(scenes)

###################################################
@app.route('/start', methods=['GET'])
def start():
	activity = {}
	activity['id'] = -1
	activity['app'] = 'START'
	activity['status'] = 'START'
	manage(activity)
	return 'OK\n'

###################################################
def manage(activity):
	global MAX_THREADS,CUR_THREADS,ACTIVITIES,s2users
	#ACTIVITIES['downloadS2']['maximum'] = getS2Users()
	app.logger.warning('manage start - lock : {} CUR_THREADS : {} ACTIVITIES : {} activity {}'.format(redis.get('rc_lock'),CUR_THREADS,ACTIVITIES,activity))

# Create the critical region while database is modified. Leave it if sleeping time is greater than 10 units to avoid sleeping forever in a buggy situation
	countsleep = 0
	while (redis.get('rc_lock') == b'1'):
		app.logger.warning('manage - sleep : {} activity {}'.format(countsleep,activity))
		time.sleep(0.5)
		countsleep += 1
		if countsleep > 10:
			redis.set('rc_lock',0)
	redis.set('rc_lock',1)

# Check if activity just finished or the flow is starting (id = -1)
	if int(activity['id']) >= 0:

		app.logger.warning('manage going to do_update lock : {} - activity {}'.format(redis.get('rc_lock'),activity))
		activity['link'] = None
		#do_upsert('activities',activity,['id','status','link','file','start','end','elapsed','retcode','message'])
		do_update('activities',activity)
		CUR_THREADS -= 1
		if activity['app'] in ACTIVITIES:
			ACTIVITIES[activity['app']]['current'] -= 1
			
# activity just finished, lets see what must be done
	if activity['status'] == 'DONE':
# Create the next activities
# If downloadS2 finished, run sen2cor
		if activity['app'] == 'downloadS2':
			activity['id'] = None
			activity['priority'] = 2
			islevel2A = True if activity['file'].find('MSIL2A') != -1 else False
			safeL2Afull = activity['file'].replace('MSIL1C','MSIL2A')
			if not os.path.exists(safeL2Afull) and not islevel2A:
				activity['app'] = 'sen2cor'
				activity['status'] = 'NOTDONE'
			else:
				activity['priority'] = 0
				activity['app'] = 'publishS2'
				# activity['status'] = 'TOLATER'
				activity['status'] = 'NOTDONE'
			activity['message'] = ''
			activity['retcode'] = 0
			app.logger.warning('manage going to sen2cor safeL2Afull : {} - activity {}'.format(safeL2Afull,activity))
			do_upsert('activities',activity,['id','status','link','file','start','end','elapsed','retcode','message'])

# If downloadLC8 finished, run espa
		elif activity['app'] == 'downloadLC8':
			activity['id'] = None
			activity['priority'] = 2
			if not espaDone(activity):
				activity['app'] = 'espa'
				activity['status'] = 'NOTDONE'
			else:
				activity['app'] = 'publishLC8'
				# activity['status'] = 'TOLATER'
				activity['status'] = 'NOTDONE'
			activity['message'] = ''
			activity['retcode'] = 0
			do_upsert('activities',activity,['id','status','link','file','start','end','elapsed','retcode','message'])

# If sen2cor finished, publish scene
		elif activity['app'] == 'sen2cor':
			activity['id'] = None
			activity['priority'] = 3
			activity['app'] = 'publishS2'
			# activity['status'] = 'TOLATER'
			activity['status'] = 'NOTDONE'
			activity['message'] = ''
			activity['retcode'] = 0
			do_upsert('activities',activity,['id','status','link','file','start','end','elapsed','retcode','message'])

# If publishS2 finished, upload scene to S3
		elif activity['app'] == 'publishS2':
			activity['id'] = None
			activity['priority'] = 3
			activity['app'] = 'uploadS2'
			activity['status'] = 'NOTDONE'
			activity['message'] = ''
			activity['retcode'] = 0
			do_upsert('activities',activity,['id','status','link','file','start','end','elapsed','retcode','message'])

# If espa finished, publish scene
		elif activity['app'] == 'espa':
			activity['id'] = None
			activity['priority'] = 3
			activity['app'] = 'publishLC8'
			activity['status'] = 'NOTDONE'
			activity['message'] = ''
			activity['retcode'] = 0
			do_upsert('activities',activity,['id','status','link','file','start','end','elapsed','retcode','message'])

	elif activity['status'] == 'ERROR':
		app.logger.warning('manage - ERROR : activity {}'.format(activity))

	sql = "SELECT * FROM activities WHERE status = 'NOTDONE' ORDER BY priority,id"
	result = do_query(sql)

	for newactivity in result:
		if CUR_THREADS >= MAX_THREADS: break
		if newactivity['app'] in ACTIVITIES:
			if ACTIVITIES[newactivity['app']]['current'] >= ACTIVITIES[newactivity['app']]['maximum']:
				#app.logger.warning('manage - not yet activity {} {}'.format(ACTIVITIES[newactivity['app']],newactivity))
				continue
			else:
				if newactivity['app'] == 'downloadS2':
					ndown = 0
					for s2user in s2users:
						ndown += s2users[s2user]['count']
					if ndown >= ACTIVITIES[newactivity['app']]['maximum']:
						continue
				ACTIVITIES[newactivity['app']]['current'] += 1
		newactivity['status'] = 'DOING'
		newactivity['elapsed'] = None
		step_start = time.time()
		newactivity['start'] = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(step_start)))
		newactivity_copy = newactivity.copy()
		t = threading.Thread(target=run, args=(newactivity,))
		app.logger.warning('manage threading - lock : {} CUR_THREADS : {} MAX_THREADS : {} newactivity : {}'.format(redis.get('rc_lock'),CUR_THREADS,MAX_THREADS,newactivity))
		t.start()
		newactivity_copy['link'] = None
		newactivity_copy['retcode'] = None
		newactivity_copy['message'] = None
		#do_upsert('activities',newactivity_copy,['id','status','link','file','start','end','elapsed','retcode','message'])
		do_update('activities',newactivity_copy)
		CUR_THREADS += 1
		app.logger.warning('manage loop - lock : {} CUR_THREADS : {} MAX_THREADS : {}'.format(redis.get('rc_lock'),CUR_THREADS,MAX_THREADS))

# Leave the critical region
	redis.set('rc_lock',0)
	app.logger.warning('manage end - lock : {} CUR_THREADS : {} activity {} s2users {}'.format(redis.get('rc_lock'),CUR_THREADS,activity,s2users.keys()))
	return

###################################################
def run(activity):
# Executing command
	if activity['app'] == 'downloadS2':
		app.logger.warning('run - downloadS2')
		step_start = time.time()
		activity['start'] = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(step_start)))
		retcode = 0
		activity['status'] = 'DONE'
		activity['message'] = 'Normal Execution'
		file = downloadS2(activity)
		if file is None:
			activity['file'] = ''
			retcode = 1
			activity['status'] = 'ERROR'
			activity['message'] = 'Abormal Execution'
		activity['file'] = file
		activity['retcode'] = retcode
		step_end = time.time()
		elapsedtime = step_end - step_start
		activity['end'] = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(step_end)))
		activity['elapsed'] = str(datetime.timedelta(seconds=elapsedtime))
		manage(activity)
		return
	elif activity['app'] == 'downloadLC8':
		app.logger.warning('run - downloadLC8')
		step_start = time.time()
		activity['start'] = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(step_start)))
		retcode = 0
		file = downloadLC8(activity)
		activity['status'] = 'DONE'
		activity['message'] = 'Normal Execution'
		if file is None:
			activity['file'] = ''
			retcode = 1
			activity['status'] = 'ERROR'
			activity['message'] = 'Abormal Execution'
		activity['file'] = file
		activity['retcode'] = retcode
		step_end = time.time()
		elapsedtime = step_end - step_start
		activity['end'] = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(step_end)))
		activity['elapsed'] = str(datetime.timedelta(seconds=elapsedtime))
		manage(activity)
		return
	elif activity['app'] == 'publishLC8':
		app.logger.warning('run - publishLC8')
		step_start = time.time()
		activity['start'] = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(step_start)))
		retcode = 0
		activity['status'] = 'DONE'
		activity['message'] = 'Normal Execution'
		retcode = publishLC8(activity)
		if retcode != 0:
			activity['file'] = ''
			activity['status'] = 'ERROR'
			activity['message'] = 'Abormal Execution'
		activity['retcode'] = retcode
		step_end = time.time()
		elapsedtime = step_end - step_start
		activity['end'] = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(step_end)))
		activity['elapsed'] = str(datetime.timedelta(seconds=elapsedtime))
		manage(activity)
		return
	elif activity['app'] == 'publishS2':
		app.logger.warning('run - publishS2')
		step_start = time.time()
		activity['start'] = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(step_start)))
		retcode = 0
		activity['status'] = 'DONE'
		activity['message'] = 'Normal Execution'
		retcode = publishS2(activity)
		if retcode != 0:
			activity['file'] = ''
			activity['status'] = 'ERROR'
			activity['message'] = 'Abormal Execution'
		activity['retcode'] = retcode
		step_end = time.time()
		elapsedtime = step_end - step_start
		activity['end'] = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(step_end)))
		activity['elapsed'] = str(datetime.timedelta(seconds=elapsedtime))
		manage(activity)
		return
	elif activity['app'] == 'uploadS2':
		app.logger.warning('run - uploadS2')
		step_start = time.time()
		activity['start'] = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(step_start)))
		retcode = 0
		activity['status'] = 'DONE'
		activity['message'] = 'Normal Execution'
		retcode = uploadS2(activity)
		if retcode != 0:
			activity['file'] = ''
			activity['status'] = 'ERROR'
			activity['message'] = 'Abormal Execution'
		activity['retcode'] = retcode
		step_end = time.time()
		elapsedtime = step_end - step_start
		activity['end'] = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(step_end)))
		activity['elapsed'] = str(datetime.timedelta(seconds=elapsedtime))
		manage(activity)
		return
	cmd = ''
	for key,val in activity.items():
		if val is not None:
			cmd += key+'='+str(val)+'&'
	if activity['app'] == 'sen2cor':
		query = 'http://'+os.environ.get('SEN2COR_HOST')+'/sen2cor?'
	elif activity['app'] == 'espa':
		query = 'http://'+os.environ.get('ESPA_HOST')+'/espa?'
	query += cmd[:-1]
	app.logger.warning('run - query '+query)
	response = requests.get(query)
	app.logger.warning('run - response {}'.format(json.loads(response.text)))
	manage(json.loads(response.text))
	return


###################################################
@app.route('/test', methods=['GET'])
def test():
	global MAX_THREADS,CUR_THREADS,ACTIVITIES
	msg = 'Maestro test:\n'
	sql = "SELECT * FROM activities WHERE app = 'downloadLC8' ORDER BY id DESC"
	results = do_query(sql)
	for result in results:
		outtar = downloadLC8(result)
		if outtar is None:
			link = result['link']
			cc = link.split('/')
			sid = cc[-3]
			last = ord(sid[-1])+1
			last = chr(last)
			cc[-3] = sid[:-1]+last
			msg += '{}\n'.format(cc[-3])
			result['link'] = '/'.join(cc)
			outtar = downloadLC8(result)
			msg += '{} - {} - {}\n'.format(result['link'],result['sceneid'],outtar)
			return msg
		msg += '{} - {} - {}\n'.format(result['id'],result['sceneid'],outtar)
	"""
	id = request.args.get('id', 2513)
	sql = "SELECT * FROM activities WHERE id = {}".format(id)
	results = do_query(sql)
	#downloadLC8(results[0])
	#return msg
	sql = "SELECT * FROM activities WHERE app = 'espa' AND status='ERROR'"
	results = do_query(sql)
	for result in results:
		sceneid = result['sceneid']
		file = result['file']
		sql = "UPDATE activities SET status='NOTDONE' WHERE sceneid = '{}' AND app='downloadLC8'".format(sceneid)
		do_command(sql)
		msg += '{}-{}\n'.format(sceneid,file)
	"""
	return msg

###################################################
@app.route('/set', methods=['GET'])
def set():
	global MAX_THREADS,CUR_THREADS,ACTIVITIES
	msg = 'Maestro set:\n'
	app = request.args.get('app', 'sen2cor')
	current = int(request.args.get('current', 0))
	maximum = int(request.args.get('maximum', 4))
	if app in ACTIVITIES:
		ACTIVITIES[app]['current'] = current
		ACTIVITIES[app]['maximum'] = maximum
		if app == 'downloadS2':
			if not os.path.exists("secrets_s2.csv"):
			    return 'No secrets_s2.csv'
			fh = open('secrets_s2.csv','r')
			line = fh.readline()
			line = fh.readline()
			line = line.strip()
			cc = line.split(",")
			s2_user = str(cc[0])
			s2_pass = str(cc[1])

			s2users[s2_user] = {'password':s2_pass,'count':0}
			msg += 's2users = {}\n'.format(s2_users.keys())
		msg += '{} = {}\n'.format(app,ACTIVITIES[app])
	return msg


###################################################
@app.route('/restart', methods=['GET'])
def restart():
	global MAX_THREADS,CUR_THREADS,ACTIVITIES
	msg = 'Rc_Maestro restarting:\n'
	id = request.args.get('id', None)
	if id is None:
		sql = "UPDATE activities SET status='NOTDONE' WHERE (status = 'ERROR' OR status = 'DOING' OR status = 'SUSPEND')"
	else:
		sql = "UPDATE activities SET status='NOTDONE' WHERE id = {}".format(id)
	do_command(sql)
	msg += 'sql - {}\n'.format(sql)
	CUR_THREADS = 0
	setActivities()
	msg += 'ACTIVITIES - {}\n'.format(ACTIVITIES)

	start()
	return msg


###################################################
@app.route('/reset', methods=['GET'])
def reset():
	global MAX_THREADS,CUR_THREADS,ACTIVITIES,s2users
	s2users = {} 
	getS2Users()
	msg = 'Rc_Maestro reseting:\n'
	sql = "UPDATE activities SET status='NOTDONE' WHERE status = 'DOING' "
	do_command(sql)
	msg += 'sql - {}\n'.format(sql)

	setActivities()
	redis.set('rc_lock',0)
	msg = 'Rc_Maestro reseting:\n'
	status = request.args.get('status', None)
	lock = getLock()
	msg += 'lock is: {}\n'.format(lock)
	msg += 'MAX_THREADS is: {}\n'.format(MAX_THREADS)
	CUR_THREADS = 0
	msg += 'CUR_THREADS is: {}\n'.format(CUR_THREADS)
	msg += 'ACTIVITIES is: {}\n'.format(ACTIVITIES)

	start()
	return msg


###################################################
@app.route('/inspect', methods=['GET'])
def inspect():
	global MAX_THREADS,CUR_THREADS,ACTIVITIES,s2users
	msg = 'Maestro Processing:\n'
	status = request.args.get('status', None)
	lock = getLock()
	msg += 'lock is: {}\n'.format(lock)
	msg += 'MAX_THREADS is: {}\n'.format(MAX_THREADS)
	msg += 'CUR_THREADS is: {}\n'.format(CUR_THREADS)
	msg += 'ACTIVITIES is: {}\n'.format(ACTIVITIES)
	msg += 's2users is: {}\n'.format(s2users.keys())
	
	if status is not None:
		sql = "SELECT * FROM activities WHERE status = '{}' ORDER BY id".format(status)
	else:
		sql = "SELECT * FROM activities ORDER BY id"
	result = do_query(sql)

	for activity in result:
		msg += '{} - {} - {} -> {}\n'.format(activity['id'],activity['app'],activity['sceneid'],activity['status'])
	return msg
	

###################################################
@app.errorhandler(400)
def handle_bad_request(e):
	resp = jsonify({'code': 400, 'message': 'Bad Request - {}'.format(e.description)})
	resp.status_code = 400
	resp.headers.add('Access-Control-Allow-Origin', '*')
	return resp


@app.errorhandler(404)
def handle_page_not_found(e):
	resp = jsonify({'code': 404, 'message': 'Page not found'})
	app.logger.info('code: 404 : Page not found')
	resp.status_code = 404
	resp.headers.add('Access-Control-Allow-Origin', '*')
	return resp


@app.errorhandler(500)
def handle_api_error(e):
	resp = jsonify({'code': 500, 'message': 'Internal Server Error'})
	resp.status_code = 500
	resp.headers.add('Access-Control-Allow-Origin', '*')
	return resp


@app.errorhandler(502)
def handle_bad_gateway_error(e):
	resp = jsonify({'code': 502, 'message': 'Bad Gateway'})
	resp.status_code = 502
	resp.headers.add('Access-Control-Allow-Origin', '*')
	return resp


@app.errorhandler(503)
def handle_service_unavailable_error(e):
	resp = jsonify({'code': 503, 'message': 'Service Unavailable'})
	resp.status_code = 503
	resp.headers.add('Access-Control-Allow-Origin', '*')
	return resp


@app.errorhandler(Exception)
def handle_exception(e):
	app.logger.exception(e)
	resp = jsonify({'code': 500, 'message': 'Internal Server Error'})
	resp.status_code = 500
	resp.headers.add('Access-Control-Allow-Origin', '*')
	return resp
