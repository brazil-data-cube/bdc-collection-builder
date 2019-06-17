import os
import io
from flask import Flask, request, make_response, render_template, abort, jsonify
import sqlalchemy
import fnmatch
import glob
import logging
import random
import time
import datetime
import requests
import subprocess
import csv
import json
import numpy
import scipy
import scipy.cluster
from scipy import ndimage
from scipy.spatial import distance
from numpngw import write_png
import skimage
from skimage import exposure
from skimage.transform import resize
from skimage import morphology
from sklearn.cluster import KMeans,Birch,AgglomerativeClustering
from sklearn.ensemble import RandomForestClassifier
from osgeo import gdal
from osgeo import osr
from osgeo import ogr
from osgeo.gdalconst import *
import zipfile
import utils
from  utils import c2jyd,do_insert,do_update,do_upsert,do_query,do_command,decodePeriods,decodePathRow
import shutil

app = Flask(__name__)
app.config['PROPAGATE_EXCEPTIONS'] = True
app.logger_name = "publisher"
handler = logging.FileHandler('soloist.log')
handler.setFormatter(logging.Formatter(
    '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
))

app.logger.addHandler(handler)

config = None

###################################################
def loadConfig():
	global config
	if config is not None: return True
	configfile = '/ds_config/bandmap.json'
	if os.path.exists(configfile):
		app.logger.warning(configfile+' was found')
		with open(configfile) as infile:
			config = json.loads(infile.read())
		return True
	else:
		app.logger.warning(configfile+' was not found')
		return False

###################################################
def sendToMaestro(activity):
# Sending command to maestro
	cmd = ''
	for key,val in activity.items():
		if val is not None:
			cmd += key+'='+str(val)+'&'
	query = 'http://'+os.environ.get('MAESTRO_HOST')+'/manage?'
	query += cmd[:-1]
	app.logger.warning(query)
	try:
		r = requests.get(query)
	except requests.exceptions.ConnectionError:
		app.logger.exception('sendToMaestro - Connection Error.')
	return

################################
def readImage(filename):
# Open source dataset
	raster = None
	try:
		mdataset = gdal.Open(filename)
# Get the image
		raster = mdataset.GetRasterBand(1).ReadAsArray(0, 0, mdataset.RasterXSize, mdataset.RasterYSize) 
	except:
		return None
	return raster

################################
def getMask(filename,dataset):
	driver = gdal.GetDriverByName('GTiff')
# Name of mask file
	masked = filename.replace('quality.tif','mask.tif')
	if os.path.exists(masked):
		dataset = gdal.Open(masked)
		if dataset is None:
			app.logger.warning('getMask -  corrupt masked {}'.format(masked))
			return None
		rastercmband = dataset.GetRasterBand(1)
		if rastercmband is None:
			app.logger.warning('getMask -  corrupt band masked {}'.format(masked))
			return None
		rastercm = rastercmband.ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)
		if rastercm is None:
			app.logger.warning('getMask -  corrupt raster masked {}'.format(masked))
			return None
		dataset = None
		return rastercm.astype(numpy.uint16)

	app.logger.warning('getMask -  masked {}'.format(masked))
# Open source dataset
	mdataset = gdal.Open(filename)
	geotransform = mdataset.GetGeoTransform()
	scenesrs = osr.SpatialReference()
	scenesrs.ImportFromWkt(mdataset.GetProjection())
	sceneproj4 = scenesrs.ExportToProj4()
# Get the image
	raster = mdataset.GetRasterBand(1).ReadAsArray(0, 0, mdataset.RasterXSize, mdataset.RasterYSize) 

# Output Cloud Mask codes
# 0 - fill
# 1 - clear data
# 0 - cloud

	if dataset == 'LC8SR':

# Input pixel_qa codes
		fill    = 1 				# warped images have 0 as fill area
		terrain = 2					# 0000 0000 0000 0010
		radsat  = 4+8				# 0000 0000 0000 1100
		cloud   = 16+32+64			# 0000 0000 0110 0000
		shadow  = 128+256			# 0000 0001 1000 0000
		snowice = 512+1024			# 0000 0110 0000 0000
		cirrus  = 2048+4096			# 0001 1000 0000 0000

		unique, counts = numpy.unique(raster, return_counts=True)
		for i in range(0,unique.shape[0]):
			app.logger.warning('getMask  -  Level {0}  \t{0:#018b} counts {1} '.format(unique[i],counts[i]))
			if unique[i] & terrain != 0:
				app.logger.warning('\t -  terrain {0:9d} \t{0:#018b}- {1}'.format(unique[i] & terrain,'2'))
			if unique[i] & radsat  != 0: 
				app.logger.warning('\t -  radsat  {0:9d} \t{0:#018b}- {1}'.format(unique[i] & radsat,'4+8'))
			if unique[i] & cloud   != 0: 
				app.logger.warning('\t -  cloud   {0:9d} \t{0:#018b}- {1}'.format(unique[i] & cloud,'16+32+64'))
			if unique[i] & shadow  != 0: 
				app.logger.warning('\t -  shadow  {0:9d} \t{0:#018b}- {1}'.format(unique[i] & shadow,'128+256'))
			if unique[i] & snowice != 0: 
				app.logger.warning('\t -  snowice {0:9d} \t{0:#018b}- {1}'.format(unique[i] & snowice,'512+1024'))
			if unique[i] & cirrus != 0:  
				app.logger.warning('\t -  cirrus  {0:9d} \t{0:#018b}- {1}'.format(unique[i] & cirrus,'2048+4096'))
			if unique[i] & fill != 0: 
				app.logger.warning('\t -  fill {0:9d} \t{0:#018b}- {1}'.format(unique[i] & fill,'1'))

# Start with a zeroed image imagearea
		imagearea = numpy.zeros(raster.shape, dtype=numpy.bool_)
# Mark with True the pixels that contain valid data
		imagearea = imagearea + raster > fill
# Create a notcleararea mask with True where the quality criteria is as follows
		notcleararea = 	(raster & radsat > 4) + \
					(raster & cloud > 64) + \
					(raster & shadow > 256) + \
					(raster & snowice > 512) + \
					(raster & cirrus > 4096) 

		strel = morphology.selem.square(6)
		notcleararea = morphology.binary_dilation(notcleararea,strel)
		morphology.remove_small_holes(notcleararea, area_threshold=80, connectivity=1, in_place=True)

	# Clear area is the area with valid data and with no Cloud or Snow
		cleararea = imagearea * numpy.invert(notcleararea)
	# Code the output image rastercm as the output codes
		rastercm = (2*notcleararea + cleararea).astype(numpy.uint16)


	elif dataset == 'MOD13Q1' or dataset == 'MYD13Q1':
#MOD13Q1 Pixel Reliability !!!!!!!!!!!!!!!!!!!! Note that 1 was added to this image in downloadModis because of warping
# Rank/Key Summary QA 		Description
# -1 		Fill/No Data 	Not Processed
# 0 		Good Data 		Use with confidence
# 1 		Marginal data 	Useful, but look at other QA information
# 2 		Snow/Ice 		Target covered with snow/ice
# 3 		Cloudy 			Target not visible, covered with cloud
		fill    = 0 	# warped images have 0 as fill area
		lut = numpy.array([0,1,1,2,2],dtype=numpy.uint8)
		rastercm = numpy.take(lut,raster).astype(numpy.uint16)

	elif dataset == 'S2SR':
# S2 sen2cor - The generated classification map is specified as follows:
# Label Classification
# 0		NO_DATA
# 1		SATURATED_OR_DEFECTIVE
# 2		DARK_AREA_PIXELS
# 3		CLOUD_SHADOWS
# 4		VEGETATION
# 5		NOT_VEGETATED
# 6		WATER
# 7		UNCLASSIFIED
# 8		CLOUD_MEDIUM_PROBABILITY
# 9		CLOUD_HIGH_PROBABILITY
#10		THIN_CIRRUS
#11		SNOW               0 1 2 3 4 5 6 7 8 9 10 11
		lut = numpy.array([0,0,2,2,1,1,1,2,2,2,1, 1],dtype=numpy.uint16)
		rastercm = numpy.take(lut,raster).astype(numpy.uint16)

	elif dataset == 'CB4_AWFI' or dataset == 'CB4_MUX':
# Key 		Summary QA 		Description
# 0 		Fill/No Data 	Not Processed
# 127 		Good Data 		Use with confidence
# 255 		Cloudy 			Target not visible, covered with cloud
		fill = 0 		# warped images have 0 as fill area
		lut = numpy.zeros(256,dtype=numpy.uint8)
		lut[127] = 1
		lut[255] = 2
		rastercm = numpy.take(lut,raster).astype(numpy.uint16)

	unique, counts = numpy.unique(rastercm, return_counts=True)
	for i in range(0,unique.shape[0]):
		app.logger.warning('getMask -  i {} unique {} counts {}'.format(i,unique[i],counts[i]))

	cmdataset = driver.Create( masked, mdataset.RasterXSize, mdataset.RasterYSize, 1, gdal. GDT_UInt16,  options = [ 'COMPRESS=LZW','TILED=YES' ] )
	# Set the geo-transform to the dataset
	cmdataset.SetGeoTransform([geotransform[0], geotransform[1], 0, geotransform[3], 0, geotransform[5]])
	# Create a spatial reference object for the dataset
	datasetsrs = osr.SpatialReference()
	datasetsrs.ImportFromWkt(mdataset.GetProjection())
	cmdataset.SetProjection(datasetsrs.ExportToWkt())
	cmdataset.GetRasterBand(1).WriteArray( rastercm )
	cmdataset = None
	raster = None
	notcleararea = None
	cleararea = None
	imagearea = None
	return rastercm


################################
def openSearchINPE(activity):
# Get datacube params
	datacube = {}
	sql = "SELECT * FROM datacubes WHERE datacube = '{}'".format(activity['datacube'])
	result = do_query(sql)
	if len(result) > 0:
		for key,val in result[0].items():
			datacube[key] = val
	bands = datacube['bands'].split(',')
# Get wrs params
	sql = "SELECT * FROM wrs WHERE name = '{}' AND tileid = '{}'".format(datacube['wrs'],activity['tileid'])
	result = do_query(sql)
	if len(result) == 0:
		return 1,'No wrs {} for tileid {}'.format(datacube['wrs'],activity['tileid'])

# Get the bounding box
	scene = result[0]
	w = scene['lonmin']
	e = scene['lonmax']
	s = scene['latmin']
	n = scene['latmax']
	#footprintWkt = scene['geom']
	#footprintPoly = ogr.CreateGeometryFromWkt(footprintWkt)
	#footprintJson = footprintPoly.ExportToJson()
	#app.logger.warning('footprintJson - {}'.format(footprintJson))

	satsens = datacube['satsen'].split(',')
	for satsen in satsens:
		if satsen.find('CB4') == 0:
			query = 'http://www.dpi.inpe.br/opensearch/granule.json?dataset={0}'.format(satsen)
		else:
			query = 'http://www.dpi.inpe.br/datasearch/granule.json?dataset={0}'.format(satsen)
		query += '&bbox={0},{1},{2},{3}'.format(w,s,e,n)
		query += '&start={0}'.format(activity['start'])
		query += '&end={0}'.format(activity['end'])
		#query += '&type=SCENE'
		query += '&count=200'
		app.logger.warning('openSearchINPE {}'.format(query))
		r = requests.get(query)
		if r.status_code != 200:
			return r.status_code,'Error in connection'
		else:
			scene = {}
			scene['datacube'] = activity['datacube']
			scene['tileid'] = activity['tileid']
			scene['start'] = activity['start']
			scene['end'] = activity['end']
			scene['dataset'] = satsen
			if satsen == 'LC8SR':
				scene['resolution'] = 30
			elif satsen == 'CB4_AWFI':
				scene['resolution'] = 64
			elif satsen == 'CB4_MUX':
				scene['resolution'] = 20
			elif satsen == 'S2SR':
				scene['resolution'] = 10
			elif satsen == 'MOD13Q1':
				scene['resolution'] = 231
			elif satsen == 'MYD13Q1':
				scene['resolution'] = 231

			r_dict = json.loads(r.text)
			#app.logger.warning('openSearchINPE totalResults {}'.format(r_dict['totalResults']))

			for k in r_dict['features']:
				if k['type'] == 'Feature':
					identifier = k['properties']['title']
					scene['sceneid '] = identifier
					date = k['properties']['date'][0:10].replace('-','')
					scene['date'] = k['properties']['date'][0:10]
					scene['cloud'] = float(k['properties']['cloudcoverq1'])

					scene['pathrow'] = '{0:03d}{1:03d}'.format(int(k['properties']['path']),int(k['properties']['row']))
					scene['cloud'] = (int(k['properties']['cloudcoverq1'])+int(k['properties']['cloudcoverq2'])+int(k['properties']['cloudcoverq3'])+int(k['properties']['cloudcoverq4']))/4
# Get file link and name
					links = k['properties']['enclosure']
					countbands = 0
					for link in links:
						band = link['band']
						if band not in bands: continue
						radiometric_processing = link['radiometric_processing']
						if radiometric_processing == 'DN' or radiometric_processing == 'TOA': continue
						scene['type'] = 'SCENE'
						scene['band'] = band
						scene['enabled'] = 1
						scene['cloudratio'] = 0
						scene['clearratio'] = 0
						scene['efficacy'] = 0
						scene['warped'] = ''
						scene['link'] = link['url']
						countbands += 1
						do_upsert('scenes',scene,['enabled','cloudratio','clearratio','efficacy','warped'],False)
# Check if there are missing bands. If so, set enabled to 2
					if countbands != len(bands):
						sql = "UPDATE scenes SET enabled = 2 WHERE sceneid = '{}'".format(identifier)
						do_command(sql)
	return 0,'Normal execution'


################################
def getMaskStats(mask):
	totpix   = mask.size
	fillpix  = numpy.count_nonzero(mask==0)
	clearpix = numpy.count_nonzero(mask==1)
	cloudpix = numpy.count_nonzero(mask==2)
	imagearea = clearpix+cloudpix
	clearratio = 0
	cloudratio = 100
	if imagearea != 0:
		clearratio = round(100.*clearpix/imagearea,1)
		cloudratio = round(100.*cloudpix/imagearea,1)
	efficacy = round(100.*clearpix/totpix,2)
	return (cloudratio,clearratio,efficacy)


###################################################
def doBlend(activity):
# Get the template info for this tile

	template = {}
	sql = "SELECT * FROM datacubes WHERE datacube = '{}' ".format(activity['datacube'])
	result = do_query(sql,True)
	for key,val in result.items():
		template[key] = val


	sql = "SELECT * FROM wrs WHERE name = '{}' AND tileid = '{}'".format(template['wrs'],activity['tileid'])
	result = do_query(sql,True)
	for key in ['xmin','ymax','srs']:
		template[key] = result[key]

	sql = "SELECT * FROM mosaics WHERE datacube = '{}' AND tileid = '{}'".format(activity['datacube'],activity['tileid'])
	result = do_query(sql,True)
	for key in ['numcol','numlin']:
		template[key] = result[key]
	app.logger.warning('warpImageToTemplate template - {}'.format(template))

# Get which scenes are related to this mosaic and should be blended
	params = "(type = 'MERGED' OR type = 'SCENE') AND enabled = 1 AND efficacy > 5."
	for key in ['datacube','tileid','start','end','band']:
		params += " AND {} = '{}'".format(key,activity[key])
			
	sql = "SELECT * FROM scenes WHERE {} ORDER by resolution ASC,efficacy DESC".format(params)
	scenes = do_query(sql)
	if len(scenes) == 0:
		return 1,'No scene for activity'
	numscenes = len(scenes)
	band = activity['band']

# The general name for the blended scene and location
	driver = gdal.GetDriverByName('GTiff')
	gdal.SetConfigOption('COMPRESS_OVERVIEW', 'LZW')
	dir = '/Repository/Mosaic/{}/{}/{}-{}'.format(activity['datacube'],activity['tileid'],activity['start'],activity['end'])
	if not os.path.exists(dir):
		os.makedirs(dir)
	generalSceneId = '{}_{}_{}'.format(activity['datacube'],activity['tileid'],activity['start'])
	generalSceneFile = os.path.join(dir,'{}_{}_*.tif'.format(generalSceneId,band))

# Already done?
	tiffs = glob.glob(generalSceneFile)
	#if len(tiffs) > 0:
	#	return 0,'Normal execution'

# Build the stack to store all images as a masked array. At this stage the array will contain the masked data	
	rasterStack = numpy.ma.zeros((numscenes,template['numlin'],template['numcol']),dtype=numpy.uint16)

# Build the raster to store the output STACK image.		
	outputraster = numpy.zeros(shape=(template['numlin'],template['numcol']),dtype=numpy.uint16)
# notdonemask will keep track of pixels that have not been filled in each step
	notdonemask = numpy.ones(shape=(template['numlin'],template['numcol']),dtype=numpy.bool_)
	notdone = numpy.count_nonzero(notdonemask)
	app.logger.warning('doBlend notdone {}'.format(notdone))

# Start filling rasterStack with the mask image and the band image
	order = 0
	for scene in scenes:
		scenename = scene['warped']
		raster = readImage(scenename)
		if raster is None:
			return 1,'No scene {}'.format(scenename)
		maskname = scenename.replace(band,'mask')
		mask = readImage(maskname)
		if mask is None:
			return 1,'No mask {}'.format(maskname)
		mask[mask!=1] = 0
		bmask = mask.astype(numpy.bool_)
# Use the mask to mark the fill (0) and cloudy (2) pixels
		rasterStack[order] = numpy.ma.masked_where(numpy.invert(bmask), raster)
# Evaluate the STACK image
# Pixels that have been already been filled by previous rasters will be masked in the current raster
		todomask = notdonemask * bmask
		notdonemask = notdonemask * numpy.invert(bmask)
		outputraster += raster*todomask
		notdone = numpy.count_nonzero(notdonemask)
		todo = numpy.count_nonzero(todomask)
		marked = numpy.count_nonzero(bmask)
		app.logger.warning('doBlend order {} notdone {} todo {} marked {} scenename - {} maskname {}'.format(order,notdone,todo,marked,scenename,maskname))
		order += 1
	newscene = scenes[0].copy()
	newscene['id'] = None
	newscene['sceneid'] = generalSceneId
	newscene['enabled'] = 1
	newscene['warped'] = None
	newscene['link'] = ''
	newscene['cloud'] = 0
	newscene['cloudratio'] = 0
	newscene['clearratio'] = 100
	newscene['efficacy'] = 100

# Create the STACK image file
	generalSceneFile = os.path.join(dir,'{}_{}_STACK.tif'.format(generalSceneId,band))
	blenddataset = driver.Create( generalSceneFile, template['numcol'], template['numlin'], 1, gdal.GDT_UInt16,  options = [ 'COMPRESS=LZW', 'TILED=YES'] )
	blenddataset.SetGeoTransform([template['xmin'], template['resx'], 0, template['ymax'], 0, -template['resy']])
	scenesrs = osr.SpatialReference()
	scenesrs.ImportFromProj4(template['srs'])
	blenddataset.SetProjection ( scenesrs.ExportToWkt() )
	blenddataset.GetRasterBand(1).SetNoDataValue(0)
	blenddataset.GetRasterBand(1).WriteArray( outputraster )
	blenddataset.BuildOverviews('NEAREST', [2, 4, 8, 16, 32])
	blenddataset = None
	app.logger.warning('doBlend - created {}'.format(generalSceneFile))
	newscene['warped'] = generalSceneFile
	newscene['type'] = 'STACK'
	do_upsert('scenes',newscene)

# Evaluate and save the mean image
	outputraster = None
	results = []
	noScenes,rows,cols = rasterStack.shape
	rasterStack_subsets = numpy.array_split(rasterStack, 200, axis=2)
	for subset in rasterStack_subsets:
		mean_subset = numpy.ma.mean(subset,axis=0).data
		results.append(mean_subset)
	outputraster = numpy.concatenate(results,axis=1)
	generalSceneFile = os.path.join(dir,'{}_{}_MEAN.tif'.format(generalSceneId,band))
	blenddataset = driver.Create( generalSceneFile, template['numcol'], template['numlin'], 1, gdal.GDT_UInt16,  options = [ 'COMPRESS=LZW','TILED=YES' ] )
	blenddataset.SetGeoTransform([template['xmin'], template['resx'], 0, template['ymax'], 0, -template['resy']])
	scenesrs = osr.SpatialReference()
	scenesrs.ImportFromProj4(template['srs'])
	blenddataset.SetProjection ( scenesrs.ExportToWkt() )
	blenddataset.GetRasterBand(1).SetNoDataValue(0)
	blenddataset.GetRasterBand(1).WriteArray( outputraster )
	blenddataset.BuildOverviews('NEAREST', [2, 4, 8, 16, 32])
	blenddataset = None
	app.logger.warning('doBlend - created {}'.format(generalSceneFile))
	newscene['warped'] = generalSceneFile
	newscene['type'] = 'MEAN'
	do_upsert('scenes',newscene)

# Evaluate and save the median image
	outputraster = None
	results = []
	noScenes,rows,cols = rasterStack.shape
	rasterStack_subsets = numpy.array_split(rasterStack, 200, axis=2)
	for subset in rasterStack_subsets:
		median_subset = numpy.ma.median(subset,axis=0).data
		results.append(median_subset)
	outputraster = numpy.concatenate(results,axis=1)
	generalSceneFile = os.path.join(dir,'{}_{}_MEDIAN.tif'.format(generalSceneId,band))
	blenddataset = driver.Create( generalSceneFile, template['numcol'], template['numlin'], 1, gdal.GDT_UInt16,  options = [ 'COMPRESS=LZW','TILED=YES' ] )
	blenddataset.SetGeoTransform([template['xmin'], template['resx'], 0, template['ymax'], 0, -template['resy']])
	scenesrs = osr.SpatialReference()
	scenesrs.ImportFromProj4(template['srs'])
	blenddataset.SetProjection ( scenesrs.ExportToWkt() )
	blenddataset.GetRasterBand(1).SetNoDataValue(0)
	blenddataset.GetRasterBand(1).WriteArray( outputraster )
	blenddataset.BuildOverviews('NEAREST', [2, 4, 8, 16, 32])
	blenddataset = None
	app.logger.warning('doBlend - created {}'.format(generalSceneFile))
	newscene['warped'] = generalSceneFile
	newscene['type'] = 'MEDIAN'
	do_upsert('scenes',newscene)

	outputraster = None
	rasterStack = None
	return 0,'Normal execution'


###################################################
def doMerge(activity):
# Get which scenes are related to this mosaic
	params = "type = 'SCENE' AND enabled = 1"
	for key in ['datacube','tileid','start','end']:
		params += " AND {} = '{}'".format(key,activity[key])
			
	sql = "SELECT * FROM scenes WHERE {}".format(params)
	scenes = do_query(sql)

# Fill the dict scenesByDay with scenes from result
	scenesByDay = {}
	for scene in scenes:
		dataset = scene['dataset']
		band = scene['band']
		date = scene['date'].strftime("%Y-%m-%d")
		if  dataset not in scenesByDay:
			scenesByDay[dataset] = {}
		if  band not in scenesByDay[dataset]:
			scenesByDay[dataset][band] = {}
		if date not in scenesByDay[dataset][band]:
			scenesByDay[dataset][band][date] = {'count':0,'scenes':[]}
		scenesByDay[dataset][band][date]['scenes'].append(scene)
		scenesByDay[dataset][band][date]['count'] += 1
	
# Scenes acquired on the sane day will be merged in one general scene in a single file
	driver = gdal.GetDriverByName('GTiff')
	gdal.SetConfigOption('COMPRESS_OVERVIEW', 'LZW')
	wdataset = None
	for dataset in sorted(scenesByDay):
		mergedscenes = {}
		for band in sorted(scenesByDay[dataset]):
			for date in sorted(scenesByDay[dataset][band]):
				count = scenesByDay[dataset][band][date]['count']
# The general name for the scene
				generalSceneId = '{}-{}-{}_M_{}'.format(dataset,activity['tileid'],date.replace('-',''),count)
# A single scene per day will not be merged
				app.logger.warning('doMerge - {} - {} {} scenes'.format(generalSceneId,count,len(scenesByDay[dataset][band][date]['scenes'])))
				if count < 2:
					continue
				gband = 'mask' if band == 'quality' else band
				generalSceneFile = os.path.join(os.path.dirname(scenesByDay[dataset][band][date]['scenes'][0]['warped']),'{}_{}.tif'.format(generalSceneId,gband))

# Do merge - rasterMask keeps track of pixels not yet inserted into rasterMerge
				rasterMerge = None
				rasterMask  = None
				order = 0
				for swarped in scenesByDay[dataset][band][date]['scenes']:
# Update enabled field in scenes table - this scene is more enbled
					id = swarped['id']
					sql = "UPDATE scenes SET enabled = 0 WHERE id = '{}'".format(id)
					do_command(sql)
					warped = swarped['warped']
					app.logger.warning('doMerge - {} {}/{}- reading warped {}'.format(activity['id'],order,count,warped))
					wdataset = gdal.Open(warped)
					raster = wdataset.GetRasterBand(1).ReadAsArray(0, 0, wdataset.RasterXSize, wdataset.RasterYSize).astype(numpy.uint16)
					if rasterMerge is None:
						rasterMerge = numpy.zeros((wdataset.RasterYSize,wdataset.RasterXSize),dtype=numpy.uint16)
						rasterMask  = numpy.ones((wdataset.RasterYSize,wdataset.RasterXSize),dtype=numpy.uint16)
					rasterMerge = rasterMerge + raster*rasterMask
					rasterMask[raster!=0] = 0
					order += 1
				app.logger.warning('doMerge - {} - creating {}'.format(activity['id'],generalSceneFile))
				if os.path.exists(generalSceneFile):
					os.remove(generalSceneFile)
					app.logger.warning('doMerge - {} - exists {}'.format(activity['id'],generalSceneFile))
				mergeddataset = driver.Create( generalSceneFile, wdataset.RasterXSize, wdataset.RasterYSize, 1, gdal.GDT_UInt16,  options = [ 'COMPRESS=LZW','TILED=YES' ] )
				mergeddataset.SetGeoTransform( wdataset.GetGeoTransform() )
				mergeddataset.SetProjection ( wdataset.GetProjection() )
				mergeddataset.GetRasterBand(1).SetNoDataValue(0)
				mergeddataset.GetRasterBand(1).WriteArray( rasterMerge )
				mergeddataset.BuildOverviews('NEAREST', [2, 4, 8, 16, 32])
				mergeddataset = None

				newscene = scenesByDay[dataset][band][date]['scenes'][0].copy()
				newscene['sceneid'] = generalSceneId
				newscene['warped'] = generalSceneFile
				newscene['enabled'] = 1
				newscene['type'] = 'MERGED'
				do_upsert('scenes',newscene)
				if date not in mergedscenes:
					mergedscenes[date] = {}
				if band not in mergedscenes[date]:
					mergedscenes[date][band] = newscene
					
# Update cloud params for the merged scene
		for date in mergedscenes:
			qualityfile = mergedscenes[date]['quality']['warped']
			app.logger.warning('doMerge - {} - reading mask {}'.format(activity['id'],qualityfile))
			mask = readImage(qualityfile)
			if mask is None:
				return 1,'No mask {}'.format(qualityfile)
			(cloudratio,clearratio,efficacy) = getMaskStats(mask)
			nscene = {}
			nscene['cloud'] = cloudratio
			nscene['cloudratio'] = cloudratio
			nscene['clearratio'] = clearratio
			nscene['efficacy'] = efficacy
			params = ''
			for key,val in nscene.items():
				params += "{} = {},".format(key,val)
			sql = "UPDATE scenes SET {} WHERE sceneid = '{}' AND datacube = '{}' AND tileid = '{}' ".format(params[:-1],scene['sceneid'],scene['datacube'],scene['tileid'])
			do_command(sql)
	return 0,'Normal execution'


################################
def downloadFromINPE(activity):
	global config

# Get the scene info to be downloaded
	sql = "SELECT * FROM {} WHERE sceneid = '{}'".format(activity['ttable'],activity['tsceneid'])
	app.logger.warning('downloadFromINPE sql - {}'.format(sql))
	results = do_query(sql)
	if len(results) == 0:
		return 1,'No scene'
	
	driver = gdal.GetDriverByName('GTiff')
	mdataset = None
	for result in results:
		scene = {}
		for key,val in result.items():
			scene[key] = val

# Get the link and file name
		link = scene['link']
		outfile = scene['file']
		band = scene['band']
		dataset = scene['dataset']

# If file exists, go to next band
		if os.path.exists(outfile): continue

# Download hdf if necessary
		hdffile = os.path.join(os.path.dirname(outfile),os.path.basename(link))
		if not os.path.exists(hdffile):
			try:
				response = requests.get(link, stream=True)
			except requests.exceptions.ConnectionError:
				return 1,'Connection Error'
			if 'Content-Length' not in response.headers:
				return 1,'Content-Length not found'
			down = open(hdffile, 'wb')
			for buf in response.iter_content(1024):
				if buf:
					down.write(buf)
			down.close()

		nodata = {		
			'evi': -3000,
			'ndvi': -3000,
			'blue': -1000,
			'red': -1000,
			'nir': -1000,
			'swir2': -1000,
			'quality': -1
		}

# Extract the subdataset related to the desired band
		if mdataset is None:
			mdataset = gdal.Open(hdffile,GA_ReadOnly)
			subdatasets = mdataset.GetSubDatasets()
			subdataset = gdal.Open(subdatasets[0][0],GA_ReadOnly)
			mgeotransform = subdataset.GetGeoTransform()
			mRasterXSize = subdataset.RasterXSize
			mRasterYSize = subdataset.RasterYSize
			mprojection = subdataset.GetProjection()
		j = config[dataset][band]
		subdataset = gdal.Open(subdatasets[j][0],GA_ReadOnly)
		geotransform = subdataset.GetGeoTransform()
		raster = subdataset.GetRasterBand(1).ReadAsArray(0, 0, subdataset.RasterXSize, subdataset.RasterYSize)
# Transform nodata to zero value
		subdatasettif = driver.Create( outfile, subdataset.RasterXSize, subdataset.RasterYSize, 1, gdal. GDT_UInt16,  options = [ 'COMPRESS=LZW','TILED=YES' ] )
# Set the geo-transform to the dataset
		subdatasettif.SetGeoTransform(geotransform)
# Create a spatial reference object for the dataset
		datasetsrs = osr.SpatialReference()
		datasetsrs.ImportFromWkt(subdataset.GetProjection())
		subdatasettif.SetProjection(datasetsrs.ExportToWkt())
# Add 1 to all bands
		raster += 1
		if band != 'quality':
# nodata will be set to zero. In quality band nodata goes from -1 to zero
			raster[raster < 0] = 0
		subdatasettif.GetRasterBand(1).WriteArray( raster )
		subdataset = None
		subdatasettif = None

	mdataset = None
	return 0,'Normal execution'
		

#################################
def warpImageToTemplate(activity):

# Get the scene info to be warped
	sql = "SELECT * FROM scenes WHERE id = {} AND enabled = 1".format(activity['tid'])
	app.logger.warning('warpImageToTemplate sql - {}'.format(sql))
	result = do_query(sql)
	scene = {}
	if len(result) == 0:
		return 0,'No scene for activity {}'.format(activity['tid'])
	for key,val in result[0].items():
		scene[key] = val

	sql = "SELECT * FROM datacubes WHERE datacube = '{}' ".format(activity['datacube'])
	app.logger.warning('warpImageToTemplate sql - {}'.format(sql))
	result = do_query(sql,True)
	for key in ['resx','resy','wrs']:
		scene[key] = result[key]

	sql = "SELECT * FROM wrs WHERE name = '{}' AND tileid = '{}'".format(scene['wrs'],scene['tileid'])
	result = do_query(sql,True)
	for key in ['xmin','ymax','srs']:
		scene[key] = result[key]

	sql = "SELECT * FROM mosaics WHERE datacube = '{}' AND tileid = '{}'".format(scene['datacube'],scene['tileid'])
	app.logger.warning('warpImageToTemplate sql - {}'.format(sql))
	result = do_query(sql,True)
	for key in ['numcol','numlin']:
		scene[key] = result[key]
	app.logger.warning('warpImageToTemplate scene - {}'.format(scene))

	"""
	sql = "SELECT * FROM wrs WHERE scenes.id = {} AND scenes.enabled = 1 \
AND datacubes.datacube = scenes.datacube \
AND wrs.name = datacubes.wrs \
AND wrs.tileid = scenes.tileid \
".format(activity['tid'])
	sql = "SELECT scenes.*,wrs.* FROM scenes,datacubes,wrs WHERE scenes.id = {} AND scenes.enabled = 1 \
AND datacubes.datacube = scenes.datacube \
AND wrs.name = datacubes.wrs \
AND wrs.tileid = scenes.tileid \
".format(activity['tid'])
	app.logger.warning('warpImageToTemplate sql - {}'.format(sql))
	result = do_query(sql)
	scene = {}
	if len(result) == 0:
		return 1,'No scene'
	for key,val in result[0].items():
		scene[key] = val

	sql = "SELECT * FROM mosaics WHERE tileid = '{}'".format(scene['tileid'])
	app.logger.warning('warpImageToTemplate sql - {}'.format(sql))
	result = do_query(sql)
	for key in ['numcol','numlin']:
		scene[key] = result[0][key]
	app.logger.warning('warpImageToTemplate scene - {}'.format(scene))
	"""
	band = scene['band']

	scenesrs = osr.SpatialReference()
	scenesrs.ImportFromProj4(scene['srs'])

# Get the input and warped files name
	filename = '/vsicurl/'+scene['link']
	warped = scene['warped']

# If warped file not exists, reproject the input scene
	if not os.path.exists(warped): 

# Open source dataset
		app.logger.warning('warpImageToTemplate doing filename - {}'.format(filename))

		try:
			src_ds = gdal.Open(filename)
		except:
			sql = "UPDATE scenes SET enabled = 0 WHERE id = '{}'".format(scene['id'])
			do_command(sql)
			return 1,'Error in gdal.Open({})'.format(filename)

		if src_ds is None:
			sql = "UPDATE scenes SET enabled = 0 WHERE id = '{}'".format(scene['id'])
			do_command(sql)
			return 1,'Dataset is None for {}'.format(filename)

		src_ds.GetRasterBand(1).SetNoDataValue(0)

# Now, we create an in-memory raster
		mem_drv = gdal.GetDriverByName( 'MEM' )
		tmp_ds = mem_drv.Create('', scene['numcol'], scene['numlin'], 1, gdal.GDT_UInt16)

# Set the geotransform
		tmp_ds.SetGeoTransform([scene['xmin'], scene['resx'], 0, scene['ymax'], 0, -scene['resy']])
		tmp_ds.SetProjection ( scenesrs.ExportToWkt() )
		tmp_ds.GetRasterBand(1).SetNoDataValue(0)

# Perform the projection/resampling
		if band == 'quality':
			resampling = gdal.GRA_NearestNeighbour
		else:
			resampling = gdal.GRA_Bilinear
		error_threshold = 0.125
		try:
			res = gdal.ReprojectImage( src_ds, tmp_ds, src_ds.GetProjection(), tmp_ds.GetProjection(), resampling)
		except:
# Update all bands with enable status
			sql = "UPDATE scenes SET enabled = 0 WHERE sceneid = '{}' AND datacube = '{}' AND tileid = '{}'".format(scene['sceneid'],scene['datacube'],scene['tileid'])
			do_command(sql)
			return 1,'Error on ReprojectImage'

# Create the final warped raster
		driver = gdal.GetDriverByName('GTiff')
		dst_ds = driver.CreateCopy(warped, tmp_ds,  options = [ 'COMPRESS=LZW', 'TILED=YES' ] )
		dst_ds = None
		tmp_ds = None

		app.logger.warning('warpImageToTemplate done filename - {} warped {}'.format(filename,warped))
# if band is quality lets evaluate the mask stats for the scene
	if band == 'quality':
		maskfile = warped.replace('quality.tif','mask.tif')
		dataset = scene['dataset']
		mask = getMask(warped,dataset)
		if mask is None:
			return 1,'No mask {}'.format(maskfile)
		(cloudratio,clearratio,efficacy) = getMaskStats(mask)
		nscene = {}
		nscene['cloudratio'] = cloudratio
		nscene['clearratio'] = clearratio
		nscene['efficacy'] = efficacy
		if efficacy <= 0.1:
			sql = "UPDATE scenes SET enabled = 0 WHERE sceneid = '{}' AND datacube = '{}' AND tileid = '{}'".format(scene['sceneid'],scene['datacube'],scene['tileid'])
			do_command(sql)
			return 0,'Efficacy {} is too low for {}'.format(efficacy,filename)

		params = ''
		for key,val in nscene.items():
			params += "{} = {},".format(key,val)
# Update all bands with cloud information
		sql = "UPDATE scenes SET {} WHERE sceneid = '{}' AND datacube = '{}' AND tileid = '{}' ".format(params[:-1],scene['sceneid'],scene['datacube'],scene['tileid'])
		do_command(sql)
# warped file will be _mask.tif instead of _quality.tif
		sql = "UPDATE scenes SET warped = '{}' WHERE id = {}".format(maskfile,scene['id'])
		do_command(sql)

	return 0,'Normal execution'


#########################################
def publish(activity):
	global config
# Get datacube params
	datacube = {}
	sql = "SELECT * FROM datacubes WHERE datacube = '{}'".format(activity['datacube'])
	result = do_query(sql)
	if len(result) > 0:
		for key,val in result[0].items():
			datacube[key] = val
	else:
		return 1,'Quicklook bands not defined in datacubes table'
	bands = datacube['quicklook'].split(',')
	fbands = datacube['bands'].split(',')
	
# Get wrs params
	wrs = {}
	sql = "SELECT * FROM wrs WHERE name = '{}' AND tileid = '{}'".format(datacube['wrs'],activity['tileid'])
	result = do_query(sql)
	if len(result) > 0:
		for key,val in result[0].items():
			wrs[key] = val
	else:
		return 1,'Tile not found in wrs table'

# Delete scenes from database
	params = "1"
	for key in ['datacube','tileid','start','end']:
		params += " AND {} = '{}'".format(key,activity[key])
		
	sql = "DELETE FROM products WHERE {}".format(params)
	do_command(sql)

	sql = "DELETE FROM qlook WHERE {}".format(params)
	do_command(sql)
       

# Evaluate corners coordinates in longlat
	llsrs = osr.SpatialReference()
	llsrs.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')
	tilesrs = osr.SpatialReference()
	tilesrs.ImportFromProj4(wrs['srs'])
	tile2ll = osr.CoordinateTransformation ( tilesrs, llsrs )
	(BL_Longitude, BL_Latitude, z ) = tile2ll.TransformPoint( wrs['xmin'], wrs['ymin'])
	(TL_Longitude, TL_Latitude, z ) = tile2ll.TransformPoint( wrs['xmin'], wrs['ymax'])
	(TR_Longitude, TR_Latitude, z ) = tile2ll.TransformPoint( wrs['xmax'], wrs['ymax'])
	(BR_Longitude, BR_Latitude, z ) = tile2ll.TransformPoint( wrs['xmax'], wrs['ymin'])

# Get which blended scenes are related to this mosaic
	for type in ['MEDIAN','STACK']:
		generalSceneId = '{}_{}_{}_{}'.format(activity['datacube'],activity['tileid'],activity['start'],type)
		files = {}
		for band in fbands:
			if band == 'quality': continue
			params = "type = '{}' AND band = '{}'".format(type,band)
			for key in ['datacube','tileid','start','end']:
				params += " AND {} = '{}'".format(key,activity[key])

			sql = "SELECT * FROM scenes WHERE {}".format(params)
			
			scenes = do_query(sql)
			#qlfiles.append(scenes[0]['warped'])
			files[band] = scenes[0]['warped']
# Register this band in products table
			product = {}
			product['filename'] = scenes[0]['warped']
			product['type'] = type
			product['cloud'] = scenes[0]['cloudratio']
			product['sceneid'] = generalSceneId
			product['datacube'] = activity['datacube']
			product['tileid'] = activity['tileid']
			product['start'] = activity['start']
			product['end'] = activity['end']
			product['band'] = band
			product['TL_Latitude'] = TL_Latitude
			product['TL_Longitude'] = TL_Longitude
			product['BR_Latitude'] = BR_Latitude
			product['BR_Longitude'] = BR_Longitude
			product['TR_Latitude'] = TR_Latitude
			product['TR_Longitude'] = TR_Longitude
			product['BL_Latitude'] = BL_Latitude
			product['BL_Longitude'] = BL_Longitude

			do_upsert('products',product)
		qlfiles = []
		for band in bands:
			qlfiles.append(files[band])
		pngname = generateQLook(generalSceneId,qlfiles)
		if pngname is None:
			return 1,'Error generateQLook for {}'.format(generalSceneId)

		qlook = {}
		qlook['sceneid'] = generalSceneId
		qlook['qlookfile'] = pngname
		qlook['datacube'] = activity['datacube']
		qlook['tileid'] = activity['tileid']
		qlook['start'] = activity['start']
		qlook['end'] = activity['end']
		do_upsert('qlook',qlook)

# Get which scenes are related to this mosaic and should be published
	params = "enabled = 1 AND (type = 'SCENE' OR type = 'MERGED')"
	for key in ['datacube','tileid','start','end']:
		params += " AND {} = '{}'".format(key,activity[key])
		
	sql = "SELECT DISTINCT sceneid FROM scenes WHERE {}".format(params)
	scenes = do_query(sql)
	scenesid = []
	for scene in scenes:
		scenesid.append(scene['sceneid'])

	for sceneid in scenesid:
		sql = "SELECT * FROM scenes WHERE sceneid = '{}' AND {}".format(sceneid,params)
		scenes = do_query(sql)
		files = {}
		for scene in scenes:
# Register this band in products table
			product = {}
			product['filename'] = scene['warped']
			files[scene['band']] = scene['warped']
			product['type'] = scene['type']
			product['cloud'] = scene['cloudratio']
			product['sceneid'] = sceneid
			product['datacube'] = activity['datacube']
			product['tileid'] = activity['tileid']
			product['start'] = activity['start']
			product['end'] = activity['end']
			product['band'] = scene['band']
			product['TL_Latitude'] = TL_Latitude
			product['TL_Longitude'] = TL_Longitude
			product['BR_Latitude'] = BR_Latitude
			product['BR_Longitude'] = BR_Longitude
			product['TR_Latitude'] = TR_Latitude
			product['TR_Longitude'] = TR_Longitude
			product['BL_Latitude'] = BL_Latitude
			product['BL_Longitude'] = BL_Longitude

			do_upsert('products',product)
		qlfiles = []
		for band in bands:
			if band in files:
				qlfiles.append(files[band])
			else:
				return 1,'Error in publish - no band {} for sceneid {}'.format(band,sceneid)
		pngname = generateQLook(sceneid,qlfiles)
		if pngname is None:
			return 1,'Error generateQLook for {}'.format(sceneid)
		qlook = {}
		qlook['sceneid'] = sceneid
		qlook['datacube'] = activity['datacube']
		qlook['tileid'] = activity['tileid']
		qlook['start'] = activity['start']
		qlook['end'] = activity['end']
		qlook['qlookfile'] = pngname
		do_upsert('qlook',qlook)
	return 0,'Normal execution'


#########################################
def generateQLook(generalSceneId,qlfiles):
	
	driver = gdal.GetDriverByName('GTiff')
# Basic image param
	dataset = None
	try:
		dataset = gdal.Open(qlfiles[0],GA_ReadOnly)
	except:
		return None
	dirname = os.path.dirname(qlfiles[0])
	numlin = 768
	numcol = int(float(dataset.RasterXSize)/float(dataset.RasterYSize)*numlin)
	image = numpy.ones((numlin,numcol,len(qlfiles),), dtype=numpy.uint8)
	app.logger.warning('generateQLook - Y {} X {} {}'.format(dataset.RasterYSize,dataset.RasterXSize,image.shape))
	nb = 0
	pngname = os.path.join(dirname,'{}.png'.format(generalSceneId))
	tifname = os.path.join(dirname,'{}.tif'.format(generalSceneId))
	rgbdataset = driver.Create( tifname, dataset.RasterXSize, dataset.RasterYSize, 3, gdal.GDT_UInt16,  options = [ 'COMPRESS=LZW' ] )
	rgbdataset.SetGeoTransform( dataset.GetGeoTransform() )
	rgbdataset.SetProjection ( dataset.GetProjection() )
	for file in qlfiles:
		app.logger.warning('generateQLook - file {}'.format(file))
		try:
			dataset = gdal.Open(file,GA_ReadOnly)
			raster = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)
		except:
			return None
		rgbdataset.GetRasterBand(nb+1).SetNoDataValue(0)
		rgbdataset.GetRasterBand(nb+1).WriteArray( raster )
		app.logger.warning('generateQLook - min {} max {}'.format(raster.min(),raster.max()))
		raster = scipy.misc.imresize(raster,(numlin,numcol))
		app.logger.warning('generateQLook - resize - {} min {} max {}'.format(raster.shape,raster.min(),raster.max()))
# Evaluate nodata mask
		nodata = raster == 0
# Evaluate minimum and maximum values
		if raster.min() != 0 or raster.max() != 0:
			a = numpy.array(raster.flatten())
			p1, p99 = numpy.percentile(a[a>0], (1, 99))
			app.logger.warning('generateQLook - p1 {} p99 {} min {} max {}'.format(p1,p99,raster.min(),raster.max()))
# Convert minimum and maximum values to 1,255 - 0 is nodata
			raster = exposure.rescale_intensity(raster, in_range=(p1, p99),out_range=(1,255))
			app.logger.warning('generateQLook -rescale_intensity - p1 {} p99 {} min {} max {}'.format(p1,p99,raster.min(),raster.max()))
		image[:,:,nb] = raster.astype(numpy.uint8) * numpy.invert(nodata)
		nb += 1
	app.logger.warning('generateQLook - pngname {} min {} max {} shape {}'.format(os.path.basename(pngname),image.min(),image.max(),image.shape))
	write_png(pngname, image, transparent=(0, 0, 0))
	return pngname

################################
def getForestMask(activity,template,which='forest'):
	currentyear = int(activity['start'].split('-')[0])
	previousyear = currentyear - 1
# Check if mask has been already built
	forestmask = ''
	if which == 'forest':
		forestmask = activity['dir']+'/'+'Prodes{}_{}_ForestMask.tif'.format(currentyear,activity['tileid'])
	else:
		forestmask = activity['dir']+'/'+'Prodes{}_{}_DeforestMask.tif'.format(currentyear,activity['tileid'])
	app.logger.warning( 'getForestMask activity {} which {} \ntemplate {} '.format(activity,which,template))
	if os.path.exists(forestmask):
		app.logger.warning( 'getForestMask from '+forestmask)
		dataset = gdal.Open(forestmask)
		raster = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize).astype(numpy.uint8)
		dataset = None
		return raster

# Get the paths/rows necessary to created the mask
	pathrows = []
	if template['name'] != 'LC8':
# Select LC8 tiles that intersects the tile bbox
		sql = "SELECT * FROM wrs WHERE name = 'LC8'"
		sql += " AND lonmax > {}".format(template['lonmin'])
		sql += " AND lonmin < {}".format(template['lonmax'])
		sql += " AND latmax > {}".format(template['latmin'])
		sql += " AND latmin < {}".format(template['latmax'])
		tiles = do_query(sql)
		for tile in tiles:
			pathrows.append(tile['tileid'])
	else:
			pathrows.append(activity['tileid'])
	app.logger.warning( 'getForestMask pathrows {}'.format(pathrows))

# Now, we create an in-memory raster where the mask will be rasterized
	mem_drv = gdal.GetDriverByName( 'MEM' )
	forest_ds = mem_drv.Create('', template['numcol'], template['numlin'], 1, gdal.GDT_Byte)

# Set the geotransform and projection
	forest_ds.SetGeoTransform([template['xmin'], template['resx'], 0, template['ymax'], 0, -template['resy']])
	scenesrs = osr.SpatialReference()
	scenesrs.ImportFromProj4(template['srs'])
	forest_ds.SetProjection ( scenesrs.ExportToWkt() )
	forest_ds.GetRasterBand(1).SetNoDataValue(0)

# Set rasterizeOptions
	rasterizeOptions = None
	if which == 'forest':
		rasterizeOptions = gdal.RasterizeOptions(burnValues=[1],
				where="class_name='FLORESTA' OR class_name='NUVEM'", 
				allTouched=True)
	else:
		rasterizeOptions = gdal.RasterizeOptions(burnValues=[1],
				where="class_name='d{}'".format(previousyear), 
				allTouched=True)
	
	shp_drv = ogr.GetDriverByName("ESRI Shapefile")
	for pathrow in pathrows:
	#pathrow = activity['tileid'].replace('0','')
		pathrow = pathrow.replace('0','')
		fromfile = 'http://www.dpi.inpe.br/prodesdigital/dadosn/{0}/PDigital{0}_{1}_shp.zip'.format(previousyear,pathrow)
		tofile = activity['dir']+'/'+'PDigital{}_{}_shp.zip'.format(previousyear,pathrow)
		app.logger.warning( 'getForestMask from '+fromfile+' to '+tofile)
		if not os.path.exists(tofile):
			try:
				response = requests.get(fromfile, stream=True)
			except requests.exceptions.ConnectionError:
				app.logger.warning('Connection Error')
			if 'Content-Length' not in response.headers:
				app.logger.warning('Content-Length not found')
			down = open(tofile, 'wb')
			for buf in response.iter_content(1024):
				if buf:
					down.write(buf)
			down.close()
		zip_ref = zipfile.ZipFile(tofile, 'r')
		zip_ref.extractall(activity['dir'])
		zip_ref.close()
		shpfile = activity['dir']+'/'+'{0}/PDigital{0}_{1}__pol.shp'.format(previousyear,pathrow)
		dataSource = shp_drv.Open(shpfile, 0)
		layer = dataSource.GetLayer(0)

		gdal.Rasterize(forest_ds, shpfile, options=rasterizeOptions)

# Create the final Forest Mask raster
	app.logger.warning( 'getForestMask - creating '+forestmask)
	tif_drv = gdal.GetDriverByName('GTiff')
	dst_ds = tif_drv.CreateCopy(forestmask, forest_ds,  options = [ 'COMPRESS=LZW','TILED=YES' ] )
	dst_ds = None
	raster = forest_ds.GetRasterBand(1).ReadAsArray(0, 0, forest_ds.RasterXSize, forest_ds.RasterYSize).astype(numpy.uint8)
	forest_ds = None
	return raster

################################
def detectChangeBRF(activity,template,previousFile, currentFile, type, forestmask, deforestmask):
	driver = gdal.GetDriverByName('GTiff')
# Create images list
	imlist = []
	valist = []
	fvalist = []
	dvalist = []
	threshold = activity['threshold']
	bands = activity['bands']

# Read previousFile
	dataset = None
	for file in previousFile:
		valist.append([])
		fvalist.append([])
		dvalist.append([])
		app.logger.warning('detectChangeBRF reading - {}'.format(file))
		if os.path.exists(file):
			dataset = gdal.Open(file,GA_ReadOnly)
			if dataset is None:
				return 1, 'detectChangeBRF - Could not open {}'.format(file)
		else:
			return 2, 'detectChangeBRF - File does not exist {}'.format(file)
		geotransform = dataset.GetGeoTransform()
		raster = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize).astype(numpy.float32)
		imlist.append(raster)

# Filter images
	for i,im in enumerate(imlist):
		app.logger.warning('detectChangeBRF filtering image - {}'.format(i))
		#imlist[i] = ndimage.median_filter(imlist[i], 3)

# Get forest samples 
	n_samples = 50000
	fwhere = numpy.where(forestmask > 0)
	skip =  int( len(fwhere[0]) / n_samples)
	skip = max(skip,1)
	app.logger.warning('detectChangeBRF fwhere - {}'.format(fwhere))
	for i in range(0,len(fwhere[0]),skip):
		row = fwhere[0][i]
		col = fwhere[1][i]
		for j in range(len(imlist)):
			valist[j].append(imlist[j][row,col])
			fvalist[j].append(imlist[j][row,col])
	F = numpy.asarray(fvalist).T
	fmean = (F.mean(axis=0)).astype(numpy.int)
	fstd  = (F.std(axis=0)).astype(numpy.int)
	app.logger.warning('detectChangeBRF forest - F shape - {}'.format(F.shape))
	app.logger.warning('detectChangeBRF forest - fmean {} fstd {}'.format(fmean,fstd))

# Get deforest samples
	dwhere = numpy.where(deforestmask > 0)
	skip =  int( len(dwhere[0]) / n_samples)
	skip = max(skip,1)
	app.logger.warning('detectChangeBRF dwhere - {}'.format(dwhere))
	for i in range(0,len(dwhere[0]),skip):
		row = dwhere[0][i]
		col = dwhere[1][i]
		for j in range(len(imlist)):
			valist[j].append(imlist[j][row,col])
			dvalist[j].append(imlist[j][row,col])
	D = numpy.asarray(dvalist).T
	dmean = (D.mean(axis=0)).astype(numpy.int)
	dstd  = (D.std(axis=0)).astype(numpy.int)
	app.logger.warning('detectChangeBRF deforest - D shape - {}'.format(D.shape))
	app.logger.warning('detectChangeBRF deforest - dmean {} dstd {}'.format(dmean,dstd))

# All samples in X
	X = numpy.asarray(valist).T


# Compute AgglomerativeClustering
	"""
https://scikit-learn.org/stable/modules/clustering.html#hierarchical-clustering
http://nbviewer.jupyter.org/gist/om-henners/c6c8d40389dab75cf535
	step_start = time.time()
	dendro_model = AgglomerativeClustering(n_clusters=10,linkage='single')
# Fit the model.
	dendro_model.fit_predict(X)
# Get the cluster assignments.
	labels = dendro_model.labels_
	unique, counts = numpy.unique(labels, return_counts=True)
	unique_labels = set(labels)
	deforest = []
	ranking = []
	for i in unique_labels:
		meanX = (X[labels == i].mean(axis=0)).astype(numpy.int)
		stdX  = (X[labels == i].std(axis=0)).astype(numpy.int)
		distf = 0
		distd = 0
		for j in range(len(imlist)):
			distf += (meanX[j] - fmean[j])*(meanX[j] - fmean[j])
			distd += (meanX[j] - dmeanX[j])*(meanX[j] - dmeanX[j])
		distf = sqrt(distf)
		distd = sqrt(distd)
		app.logger.warning('detectChangeBRF meanX - i {} meanX {} stdX {} counts {} distf [] distd []'.format(i,meanX,stdX,counts[i],distf,distd))
	step_end = time.time()
	elapsedtime = step_end - step_start
	ela = str(datetime.timedelta(seconds=elapsedtime))
	app.logger.warning('detectChangeBRF AgglomerativeClustering fit - elapsedtime {}'.format(ela))
	return 1, 'detectChangeBRF - Could  open {}'.format(previousFile)
	
# Compute 
	step_start = time.time()
	linkage = scipy.cluster.hierarchy.linkage(X, method='ward', metric='euclidean')
	app.logger.warning('detectChangeBRF linkage done  {} threshold {}'.format(linkage,threshold))
	labels = scipy.cluster.hierarchy.fcluster(linkage, int(threshold)*10, criterion='distance')
	nclusters = len(numpy.unique(labels))
	unique, counts = numpy.unique(labels, return_counts=True)
	unique_labels = set(labels)
	app.logger.warning('detectChangeBRF fcluster done unique_labels {}'.format(unique_labels))
	deforest = []
	forest = []
	ranking = []
	for i in unique_labels:
		meanX = (X[labels == i].mean(axis=0)).astype(numpy.int)
		stdX  = (X[labels == i].std(axis=0)).astype(numpy.int)
		distf = 0
		distd = 0
		for j in range(len(imlist)):
			distf += (meanX[j] - fmean[j])*(meanX[j] - fmean[j])
			distd += (meanX[j] - dmean[j])*(meanX[j] - dmean[j])
		distf = numpy.sqrt(distf)
		distd = numpy.sqrt(distd)
		if distf < distd:
			forest.append(i)
		else:
			deforest.append(i)
		app.logger.warning('detectChangeBRF - i {} meanX {} stdX {} counts {} distf {} distd {}'.format(i,meanX,stdX,counts[i-1],distf,distd))
	step_end = time.time()
	elapsedtime = step_end - step_start
	ela = str(datetime.timedelta(seconds=elapsedtime))
	app.logger.warning('detectChangeBRF fcluster fit - forest {}'.format(forest))
	app.logger.warning('detectChangeBRF fcluster fit - deforest {}'.format(deforest))
	app.logger.warning('detectChangeBRF fcluster - elapsedtime {} nclusters {}'.format(ela,nclusters))
	"""

# Compute Birch
	step_start = time.time()
	nclusters = 20
	birch_model = Birch(n_clusters=nclusters,threshold=int(threshold))

# Fit the model.
	birch_model.fit(X)

# Get the cluster assignments.
	labels = birch_model.labels_
	unique, counts = numpy.unique(labels, return_counts=True)
	unique_labels = set(labels)
	app.logger.warning('detectChangeBRF unique_labels {}'.format(unique_labels))
	app.logger.warning('detectChangeBRF unique {}'.format(unique))
	app.logger.warning('detectChangeBRF counts {}'.format(counts))

# Assign each cluster to forest or deforest based on its distance to forest and deforest mean
	deforest = []
	forest = []
	for i in range(0,unique.shape[0]):
		unique_label = unique[i]
		count = counts[i]
		meanX = (X[labels == unique_label].mean(axis=0)).astype(numpy.int)
		stdX  = (X[labels == unique_label].std(axis=0)).astype(numpy.int)
		distf = distance.euclidean(meanX, fmean).astype(numpy.int)
		distd = distance.euclidean(meanX, dmean).astype(numpy.int)

		if distf < distd:
			forest.append(unique_label)
		else:
			deforest.append(unique_label)
		app.logger.warning('detectChangeBRF - unique_label {} meanX {} stdX {} count {} distf {} distd {}'.format(unique_label,meanX,stdX,count,distf,distd))
	step_end = time.time()
	elapsedtime = step_end - step_start
	ela = str(datetime.timedelta(seconds=elapsedtime))
	app.logger.warning('detectChangeBRF Birch fit - forest {}'.format(forest))
	app.logger.warning('detectChangeBRF Birch fit - deforest {}'.format(deforest))
	app.logger.warning('detectChangeBRF Birch fit - elapsedtime {}'.format(ela))
	#return 1, 'detectChangeBRF - Test'

# Create matrix to store all flattened images
	immatrix = numpy.dstack(imlist)
	rows, cols, noBands = immatrix.shape
	flat_pixels = immatrix.reshape((rows*cols, noBands))
	flat_pixels_subsets = numpy.array_split(flat_pixels, 50)

# Classify the whole images by the cluster assignments.
	step_start = time.time()
	results = []
	for subset in flat_pixels_subsets:
		result_subset = birch_model.predict(subset)
		results.append(result_subset)
	result = numpy.concatenate(results)

# Clean the classified image, use only deforest labels
	lut = numpy.zeros(len(unique_labels),dtype=numpy.uint8)
	for label in unique_labels:
		lut[label] = 1 if label in deforest else 0
	result = numpy.take(lut,result)
	classified = result.reshape(rows,cols)

	elapsedtime = time.time() - step_start
	ela = str(datetime.timedelta(seconds=elapsedtime))
	app.logger.warning('detectChangeBRF Birch predict - elapsedtime {}'.format(ela))

# Mask the classification result
	totalmask = forestmask + deforestmask
	totalmask[totalmask == 2] = 1
	classified = classified * totalmask
	
# Create a classified image
	sbands = '_'.join(bands)
	classfile = activity['dir']+'/'+'Prodes_{}_{}_{}_{}_birch.tif'.format(activity['tileid'],activity['previousstart'],sbands,type)
	app.logger.warning('detectChange creating - {}'.format(classfile))
	classdataset = driver.Create( classfile, dataset.RasterXSize, dataset.RasterYSize, 1, gdal.GDT_Byte,  options = [ 'COMPRESS=LZW','TILED=YES' ] )
# Set the geo-transform to the classdataset
	classdataset.SetGeoTransform( geotransform )
# Create a spatial reference object for the dataset
	classdataset.SetProjection ( dataset.GetProjection() )
	classdataset.GetRasterBand(1).SetNoDataValue(0)
	classdataset.GetRasterBand(1).WriteArray( classified )
	classdataset = None
	classified = None
	results = None

# Perform classification by RandomForestClassifier
# http://scikit-learn.org/dev/modules/generated/sklearn.ensemble.RandomForestClassifier.html
	step_start = time.time()
	rf_model = RandomForestClassifier(n_jobs=1, n_estimators=nclusters, class_weight='balanced')
# Fit our model to training data
	rf_model.fit(X, labels)
	elapsedtime = time.time() - step_start
	ela = str(datetime.timedelta(seconds=elapsedtime))
	app.logger.warning('detectChangeBRF RandomForestClassifier fit - elapsedtime {}'.format(ela))

# Classify the whole images.
	step_start = time.time()
	results = []
	for subset in flat_pixels_subsets:
		result_subset = rf_model.predict(subset)
		results.append(result_subset)
	result = numpy.concatenate(results)

# Clean the classified image, use only deforest labels
	result = numpy.take(lut,result)
	classified = result.reshape(rows,cols)
	elapsedtime = time.time() - step_start
	ela = str(datetime.timedelta(seconds=elapsedtime))
	app.logger.warning('detectChangeBRF RandomForestClassifier predict - elapsedtime {}'.format(ela))

# Mask the classification result
	classified = classified * totalmask

# Create a classified image
	sbands = '_'.join(bands)
	classfile = activity['dir']+'/'+'Prodes_{}_{}_{}_{}_rf.tif'.format(activity['tileid'],activity['previousstart'],sbands,type)
	app.logger.warning('detectChange creating - {}'.format(classfile))
	classdataset = driver.Create( classfile, dataset.RasterXSize, dataset.RasterYSize, 1, gdal.GDT_Byte,  options = [ 'COMPRESS=LZW','TILED=YES' ] )
# Set the geo-transform to the classdataset
	classdataset.SetGeoTransform( geotransform )
# Create a spatial reference object for the dataset
	classdataset.SetProjection ( dataset.GetProjection() )
	classdataset.GetRasterBand(1).SetNoDataValue(0)
	classdataset.GetRasterBand(1).WriteArray( classified )
	classdataset = None
	classified = None
	results = None

# Now, lets classify the current year image based on model evaluated for previous year
# Read currentFile
	dataset = None
	imlist = []
	for file in currentFile:
		if os.path.exists(file):
			dataset = gdal.Open(file,GA_ReadOnly)
			if dataset is None:
				return 1, 'detectChangeBRF - Could not open {}'.format(file)
		else:
			return 2, 'detectChangeBRF - File does not exist {}'.format(file)
		geotransform = dataset.GetGeoTransform()
		raster = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize).astype(numpy.float32)
		imlist.append(raster)

# Filter images
	for i,im in enumerate(imlist):
		app.logger.warning('detectChangeBRF filtering image - {}'.format(i))
		imlist[i] = ndimage.median_filter(imlist[i], 3)

# Create matrix to store all flattened images
	immatrix = numpy.dstack(imlist)
	rows, cols, noBands = immatrix.shape
	flat_pixels = immatrix.reshape((rows*cols, noBands))
	flat_pixels_subsets = numpy.array_split(flat_pixels, 50)

# Classify the whole images by RandomForestClassifier
	step_start = time.time()
	results = []
	for subset in flat_pixels_subsets:
		result_subset = rf_model.predict(subset)
		results.append(result_subset)
	result = numpy.concatenate(results)

# Clean the classified image, use only deforest labels
	result = numpy.take(lut,result)
	classified = result.reshape(rows,cols)
	elapsedtime = time.time() - step_start
	ela = str(datetime.timedelta(seconds=elapsedtime))
	app.logger.warning('detectChangeBRF RandomForestClassifier predict - elapsedtime {}'.format(ela))

# Mask the classification result using forest mask, only forest may be deforested
	classified = classified * forestmask

# Clean the binary image
	min_size = 3
	strel = morphology.selem.square(min_size)
	strel = morphology.selem.diamond(min_size)
	#strel = morphology.selem.disk(min_size)
	classified = morphology.binary_erosion (classified,strel)
	morphology.remove_small_objects(classified, min_size=min_size, connectivity=1, in_place=True)
	classified = morphology.binary_dilation(classified,strel)
	morphology.remove_small_holes(classified, area_threshold=min_size, connectivity=1, in_place=True)

# Create a classified image
	sbands = '_'.join(bands)
	classfile = activity['dir']+'/'+'Prodes_{}_{}_{}_{}_clean_rf.tif'.format(activity['tileid'],activity['start'],sbands,type)
	app.logger.warning('detectChangeBRF creating - {}'.format(classfile))
	classdataset = driver.Create( classfile, dataset.RasterXSize, dataset.RasterYSize, 1, gdal.GDT_Byte,  options = [ 'COMPRESS=LZW','TILED=YES' ] )
# Set the geo-transform to the classdataset
	classdataset.SetGeoTransform( geotransform )
# Create a spatial reference object for the dataset
	classdataset.SetProjection ( dataset.GetProjection() )
	classdataset.GetRasterBand(1).SetNoDataValue(0)
	classdataset.GetRasterBand(1).WriteArray( classified )
	results = None

# Create a quicklook
	numlin = 768
	numcol = int(float(classdataset.RasterXSize)/float(classdataset.RasterYSize)*numlin)
	image = numpy.zeros((numlin,numcol,3,), dtype=numpy.uint8)
	app.logger.warning('detectChangeBRF - Y {} X {} {}'.format(classdataset.RasterYSize,classdataset.RasterXSize,image.shape))
	pngname = classfile.replace('tif','png')
	raster = scipy.misc.imresize(classified.astype(numpy.uint8),(numlin,numcol))
	image[:,:,0] = raster.astype(numpy.uint8)*255
	#app.logger.warning('detectChange - creating {}'.format(os.path.basename(pngname)))
	app.logger.warning('detectChangeBRF - creating {} min {} max {} shape {}'.format(os.path.basename(pngname),image.min(),image.max(),image.shape))
	write_png(pngname, image, transparent=(0, 0, 0))
	classified = None
	image = None
	raster = None

# Publish the classification
	generalSceneId = '{}_{}_{}_{}'.format(activity['datacube'],activity['tileid'],activity['start'],type)
	sql = "SELECT * FROM products WHERE sceneid = '{}'".format(generalSceneId)
	products = do_query(sql)
	generalSceneId = '{}_{}_{}_{}_CLASS'.format(activity['datacube'],activity['tileid'],activity['start'],type)
	product = products[0]
	product['sceneid'] = generalSceneId
	product['filename'] = classfile
	product['band'] = 'brf'
	product['type'] = 'CLASSIFIED'
	do_upsert('products',product)
	qlook = {}
	qlook['sceneid'] = generalSceneId
	qlook['datacube'] = activity['datacube']
	qlook['tileid'] = activity['tileid']
	qlook['start'] = activity['start']
	qlook['end'] = activity['end']
	qlook['qlookfile'] = pngname
	do_upsert('qlook',qlook)

# Create a shapefile to store polygonized data
	shapename = classfile.replace('tif','shp')
	driver = ogr.GetDriverByName("ESRI Shapefile")
	if os.path.exists(shapename):
		driver.DeleteDataSource(shapename)
	data_source = driver.CreateDataSource( shapename )
	layername = classfile.replace('.tif','')
	scenesrs = osr.SpatialReference()
	scenesrs.ImportFromProj4(template['srs'])
	layer = data_source.CreateLayer(layername, scenesrs, ogr.wkbPolygon)
	field = ogr.FieldDefn('field', ogr.OFTInteger)
	layer.CreateField(field)
# Polygonize binary data
	binband = classdataset.GetRasterBand(1)
	gdal.Polygonize( binband, binband, layer, 0, [])
	data_source.Destroy()

	classdataset = None
	return 0, 'Normal execution'

################################
def detectChange(activity,template,previousFile, currentFile, band, type, forestmask):
	driver = gdal.GetDriverByName('GTiff')
# Create images list
	imlist = []
	valist = []

# Read previousFile
	dataset = None
	app.logger.warning('detectChange reading - {}'.format(previousFile))
	if os.path.exists(previousFile):
		dataset = gdal.Open(previousFile,GA_ReadOnly)
		if dataset is None:
			return 1, 'detectChange - Could not open {}'.format(previousFile)
	else:
		return 2, 'detectChange - File does not exist {}'.format(previousFile)
	geotransform = dataset.GetGeoTransform()
	raster = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize).astype(numpy.float32)
	imlist.append(raster)

# Read currentFile
	app.logger.warning('detectChange reading - {}'.format(currentFile))
	if os.path.exists(currentFile):
		dataset = gdal.Open(currentFile,GA_ReadOnly)
		if dataset is None:
			return 1, 'detectChange - Could not open {}'.format(currentFile)
	else:
		return 2, 'detectChange - File does not exist {}'.format(currentFile)
	geotransform = dataset.GetGeoTransform()
	raster = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize).astype(numpy.float32)
	imlist.append(raster)

# Filter images
	for i,im in enumerate(imlist):
		app.logger.warning('detectChange filtering image - {}'.format(i))
		imlist[i] = ndimage.median_filter(imlist[i], 3)

# Create nodata mask for images
	m,n = imlist[0].shape[0:2] # get the size of the images
	margin = numpy.zeros((m,n,), dtype=numpy.bool_)
	for i,im in enumerate(imlist):
		app.logger.warning('detectChange masking image - {}'.format(i))
		margin = margin + numpy.invert(im != 0)
	margin = numpy.invert(margin) * forestmask

# Mask nodata in images and evaluate normalization
	for i,im in enumerate(imlist):
		valist.append([])
		a = numpy.array(im.flatten())
		a = a[numpy.logical_not(a==0)]
		p2, p98 = numpy.percentile(a, (2, 98))
		app.logger.warning('detectChange image - {} p2 {} p98 {}'.format(i,p2,p98))
		imlist[i] = exposure.rescale_intensity(imlist[i], in_range=(p2, p98),out_range=(1,1000))
		imlist[i] = imlist[i] * margin

# Create a Difference image
	difffile = activity['dir']+'/'+'Prodes_{}_{}_{}_{}_diff.tif'.format(activity['tileid'],activity['start'],band,type)
	if True or not os.path.exists(difffile):
		app.logger.warning('detectChange creating - {}'.format(difffile))
		#diffdataset = driver.Create( difffile, dataset.RasterXSize, dataset.RasterYSize, 1, gdal.GDT_Byte,  options = [ 'COMPRESS=LZW' ] )
		diffdataset = driver.Create( difffile, dataset.RasterXSize, dataset.RasterYSize, 1, gdal.GDT_Int16,  options = [ 'COMPRESS=LZW','TILED=YES' ] )
# Set the geo-transform to the diffdataset
		diffdataset.SetGeoTransform( geotransform )
# Create a spatial reference object for the dataset
		diffdataset.SetProjection ( dataset.GetProjection() )
		diffdataset.GetRasterBand(1).SetNoDataValue(0)
		im = (imlist[1] - imlist[0])
		app.logger.warning('detectChange im - {} {} {}'.format(im.min(),im.mean(),im.max()))
		#im[im >  10] = 0
		#im[im <= 10] = 1
		#im = im.astype(numpy.uint8) 
		im = im.astype(numpy.int16) 
		diffdataset.GetRasterBand(1).WriteArray( im )
		diffdataset = None
		return 1, 'detectChange - Test'

# Clean the binary image
		min_size = 4
		strel = morphology.selem.square(min_size)
		strel = morphology.selem.diamond(min_size)
		#strel = morphology.selem.disk(min_size)
		im = morphology.binary_erosion (im,strel)
		#im = morphology.binary_dilation(im,strel)
		morphology.remove_small_holes(im, area_threshold=min_size, connectivity=1, in_place=True)
		morphology.remove_small_objects(im, min_size=min_size, connectivity=1, in_place=True)
		im = morphology.binary_dilation(im,strel)

		difffile = activity['dir']+'/'+'Prodes_{}_{}_{}_{}_clean_diff.tif'.format(activity['tileid'],activity['start'],band,type)
		diffdataset = driver.Create( difffile, dataset.RasterXSize, dataset.RasterYSize, 1, gdal.GDT_Byte,  options = [ 'COMPRESS=LZW','TILED=YES' ] )
# Set the geo-transform to the diffdataset
		diffdataset.SetGeoTransform( geotransform )
# Create a spatial reference object for the dataset
		diffdataset.SetProjection ( dataset.GetProjection() )
		diffdataset.GetRasterBand(1).SetNoDataValue(0)
		diffdataset.GetRasterBand(1).WriteArray( im )

# Create a quicklook
		numlin = 768
		numcol = int(float(diffdataset.RasterXSize)/float(diffdataset.RasterYSize)*numlin)
		image = numpy.zeros((numlin,numcol,3,), dtype=numpy.uint8)
		app.logger.warning('detectChange - Y {} X {} {}'.format(diffdataset.RasterYSize,diffdataset.RasterXSize,image.shape))
		pngname = difffile.replace('tif','png')
		raster = scipy.misc.imresize(im.astype(numpy.uint8),(numlin,numcol))
		image[:,:,0] = raster.astype(numpy.uint8)*255
		#app.logger.warning('detectChange - creating {}'.format(os.path.basename(pngname)))
		app.logger.warning('detectChange - creating {} min {} max {} shape {}'.format(os.path.basename(pngname),image.min(),image.max(),image.shape))
		write_png(pngname, image, transparent=(0, 0, 0))

# Publish the difference
		generalSceneId = '{}_{}_{}_{}'.format(activity['datacube'],activity['tileid'],activity['start'],type)
		params = "band = '{}'".format(band)
		sql = "SELECT * FROM products WHERE sceneid = '{}' AND band = '{}'".format(generalSceneId,band)
		products = do_query(sql)
		generalSceneId = '{}_{}_{}_{}_CLASS'.format(activity['datacube'],activity['tileid'],activity['start'],type)
		product = products[0]
		product['sceneid'] = generalSceneId
		product['filename'] = difffile
		product['band'] = 'diff'
		product['type'] = 'CLASSIFIED'
		do_upsert('products',product)
		qlook = {}
		qlook['sceneid'] = generalSceneId
		qlook['datacube'] = activity['datacube']
		qlook['tileid'] = activity['tileid']
		qlook['start'] = activity['start']
		qlook['end'] = activity['end']
		qlook['qlookfile'] = pngname
		do_upsert('qlook',qlook)

# Create a shapefile to store polygonized data
		shapename = difffile.replace('tif','shp')
		driver = ogr.GetDriverByName("ESRI Shapefile")
		if os.path.exists(shapename):
			driver.DeleteDataSource(shapename)
		data_source = driver.CreateDataSource( shapename )
		layername = difffile.replace('.tif','')
		scenesrs = osr.SpatialReference()
		scenesrs.ImportFromProj4(template['srs'])
		layer = data_source.CreateLayer(layername, scenesrs, ogr.wkbPolygon)
		field = ogr.FieldDefn('field', ogr.OFTInteger)
		layer.CreateField(field)
# Polygonize binary data
		binband = diffdataset.GetRasterBand(1)
		gdal.Polygonize( binband, binband, layer, 0, [])
		data_source.Destroy()

		diffdataset = None

	return 0, 'Normal execution'

###################################################
@app.route('/create', methods=['GET'])
def createDataCube():
	params = {}
	for key in request.args:
		params[key] = request.args.get(key)
	#return jsonify(params)
	datacube = {}
	if 'datacube' in params:
		datacube['datacube'] = params['datacube']
	else:
		return jsonify({
			"statusCode": 200,
			"body": json.dumps({
				"message": 'Datacube name is missing (datacube)',
				"params": params
			}),
		})
	if 'wrs' in params:
		datacube['wrs'] = params['wrs']
	elif 'projection' in params:
		datacube['wrs'] = datacube['datacube']
		argsmeridian = params['meridian'] if 'meridian' in params else 0.
		argsprojection = params['projection']
		argszone = params['zone'] if 'zone' in params else 23
		
		if argsprojection == 'aea':
			tilesrsp4 = "+proj=aea +lat_1=10 +lat_2=-40 +lat_0=0 +lon_0=-50 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs".format(argsmeridian)
		elif argsprojection == 'utm':
			tilesrsp4 = '+proj=utm +zone={} +datum=WGS84 +units=m +no_defs'.format(argszone)
		elif argsprojection == 'sinu':
			tilesrsp4 = "+proj=sinu +lon_0={0} +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs".format(0.)
		elif argsprojection == 'longlat':
			tilesrsp4 = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"


# Evaluate corners coordinates in longlat
		tilesrs = osr.SpatialReference()
		tilesrs.ImportFromProj4(tilesrsp4)
		app.logger.warning('genwrs -  argsprojection {} tilesrsp4 {}'.format(argsprojection,tilesrsp4))
		llsrs = osr.SpatialReference()
		llsrs.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')
		ll2tile = osr.CoordinateTransformation ( llsrs, tilesrs )
		tile2ll = osr.CoordinateTransformation ( tilesrs, llsrs )

		wrs = {}

		wrs['name'] = datacube['datacube']
		wrs['srs'] = tilesrsp4
		wrs['tileid'] = datacube['datacube']
		if 'w' in params and 'n' in params and 'e' in params and 's' in params:
# Minimum and maximum x and y
			(xl, yu, z ) = ll2tile.TransformPoint(float(params['w']),float(params['n']))
			(xr, yb, z ) = ll2tile.TransformPoint(float(params['e']),float(params['s']))

			wrs['xmin'] = xl
			wrs['xmax'] = xr
			wrs['ymin'] = yb
			wrs['ymax'] = yu
			wrs['lonmin'] = float(params['w'])
			wrs['lonmax'] = float(params['e'])
			wrs['latmin'] = float(params['s'])
			wrs['latmax'] = float(params['n'])
			wrs['geom'] = 'POLYGON (({0} {1} 0, {2} {1} 0, {2} {3} 0, {0} {3} 0, {0} {1} 0))'.format(params['w'],params['n'],params['e'],params['s'])
			sql = "DELETE FROM wrs WHERE name = '{}'".format(wrs['name'])
			do_command(sql)
			do_insert('wrs',wrs)
			print('wrs',wrs)
		else:
			return jsonify({
				"statusCode": 200,
				"body": json.dumps({
					"message": 'Bounding Box is missing (w,n,e,s)',
					"params": params
				}),
			})
	else:
		return jsonify({
			"statusCode": 200,
			"body": json.dumps({
				"message": 'Reference System is missing (wrs or srs)',
				"params": params
			}),
		})
	if 'resx' in params and 'resy' in params:
		datacube['resx'] = float(params['resx'])
		datacube['resy'] = float(params['resy'])
	else:
		return jsonify({
			"statusCode": 200,
			"body": json.dumps({
				"message": 'Resolution is missing (resx,resy)',
				"params": params
			}),
		})

	datacube['start']  = params['start'] if 'start' in params else '2018-01-01'
	datacube['end']  = params['end'] if 'end' in params else '2018-12-31'
	datacube['tschema']  = params['tschema'] if 'tschema' in params else 'M'
	datacube['step']  = params['step'] if 'step' in params else 30
	datacube['satsen']  = params['satsen'] if 'satsen' in params else 'S2SR'
	datacube['bands']  = params['bands'] if 'bands' in params else 'ndvi,evi,swir2,swir1,nir,red,green,blue,quality'
	datacube['quicklook']  = params['quicklook'] if 'quicklook' in params else 'swir2,nir,red'
	sql = "DELETE FROM datacubes WHERE datacube = '{}'".format(datacube['datacube'])
	do_command(sql)

	do_insert('datacubes',datacube)
	print('datacube',datacube)

	sql = "SELECT * FROM datacubes WHERE datacube = '{}'".format(datacube['datacube'])
	result = do_query(sql)
	return jsonify(result)

##################################################
@app.route('/cubestatus', methods=['GET'])
def cubeStatus():
    cubename = request.args.get('cubename', None)
    sql = "SELECT count(*), status FROM `activities` WHERE datacube = '{}' group by status".format(cubename)
    result = do_query(sql)
    return jsonify(result)

##################################################
@app.route('/wrsinfo', methods=['GET'])
def wrsInfos():
    sql = "SELECT distinct(name) FROM wrs"
    result = do_query(sql)
    return jsonify(result)

##################################################
@app.route('/cubeinfo', methods=['GET'])
def cubeInfos():
	sql = "SELECT * FROM datacubes"
	result = do_query(sql)
	return jsonify(result)


###################################################
@app.route('/run', methods=['GET'])
def run():
	global config
	loadConfig()
	gdal.UseExceptions() #Exceptions will get raised on anything >= gdal.CE_Failure
	activity = {}
	activity['id'] = request.args.get('id', None)
	activity['app'] = request.args.get('app', None)
	activity['priority'] = request.args.get('priority', None)
	activity['datacube'] = request.args.get('datacube', None)
	activity['tileid'] = request.args.get('tileid', None)
	activity['start'] = request.args.get('start', None)
	activity['end'] = request.args.get('end', None)
	activity['ttable'] = request.args.get('ttable', None)
	activity['tid'] = request.args.get('tid', None)
	activity['tsceneid'] = request.args.get('tsceneid', None)
	activity['band'] = request.args.get('band', None)

	step_start = time.time()
	activity['pstart'] = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(step_start)))
	retcode = 0
	app.logger.warning('{} being called for {}'.format(activity['app'],activity))
	activity['message'] = 'Normal Execution'
	if activity['app'] == 'search':
		retcode,activity['message'] = openSearchINPE(activity)
	elif activity['app'] == 'download':
		retcode,activity['message'] = downloadFromINPE(activity)
	elif activity['app'] == 'warp':
		retcode,activity['message'] = warpImageToTemplate(activity)
	elif activity['app'] == 'merge':
		retcode,activity['message'] = doMerge(activity)
	elif activity['app'] == 'blend':
		retcode,activity['message'] = doBlend(activity)
	elif activity['app'] == 'publish':
		retcode,activity['message'] = publish(activity)
	else:
		sleep = random.randint(1, 10)
		app.logger.warning('{} sleeping for {} secs'.format(activity['app'],sleep))
		time.sleep(sleep)
		if sleep < 5: retcode = 1
		activity['message'] = 'Slept {} seconds'.format(sleep)
	step_end = time.time()
	elapsedtime = step_end - step_start
	activity['pend'] = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(step_end)))
	activity['elapsed'] = str(datetime.timedelta(seconds=elapsedtime))
	if retcode == 0:
		activity['status'] = 'DONE'
	else:
		if activity['app'] == 'warp':
			activity['status'] = 'ERROR'
		else:
			activity['status'] = 'ERROR'
	activity['retcode'] = retcode
	sendToMaestro(activity)
	return 'OK'


###################################################
@app.route('/search', methods=['GET'])
def decodeRequest():
#wlon,nlat,elon,slat,startdate,enddate,cloud,limit
	sat = request.args.get('sat', 'LC8')
	path = request.args.get('path', None)
	row  = request.args.get('row', None)
	wlon = request.args.get('wlon', -54)
	nlat = request.args.get('nlat', -13)
	elon = request.args.get('elon', -52)
	slat = request.args.get('slat', -15)
	startdate = request.args.get('startdate', '2016-12-07')
	enddate = request.args.get('enddate', datetime.datetime.now().strftime("%Y-%m-%d"))
	cloud = float(request.args.get('cloud', 0.5))
	limit = request.args.get('limit', 50)
	app.logger.warning('search - sat {} startdate {} cloud {} limit {}'.format(sat,startdate,cloud,limit))
	if sat == 'LC8':
		if path is not None and row is not None:
			result = developmentSeedPR(path,row,startdate,enddate,cloud,limit)
		else:
			result = developmentSeed(wlon,nlat,elon,slat,startdate,enddate,cloud,limit)
	elif sat == 'S2':
		app.logger.warning('search - calling openSearchS2SAFE')
		result = openSearchS2SAFE(wlon,nlat,elon,slat,startdate,enddate,cloud,limit)
	return jsonify(result)





###################################################
def sendToRadcor(activity,action):
# Sending command to radcor
	cmd = ''
	for key,val in activity.items():
		if val is not None:
			cmd += key+'='+str(val)+'&'
	query = 'http://'+os.environ.get('RADCOR_HOST')+'/{}?'.format(action)
	query += cmd[:-1]
	app.logger.warning('sendToRadcor - '+query)
	try:
		r = requests.get(query)
	except requests.exceptions.ConnectionError:
		app.logger.exception('sendToRadcor - Connection Error - '+query)
	return r.json()



###################################################
@app.route('/genwrs', methods=['GET'])
def genwrs():
	"""
# Get the arguments
parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter,description=textwrap.dedent('''WRS Generator.  ''' ))
parser.add_argument("-w", "--wrsname", default=None,help="Mosaic WRS Name",type=str)
parser.add_argument("-dx", "--degreesx", default=10.,help="Cell longitude width at Equator in degrees",type=float)
parser.add_argument("-dy", "--degreesy", default=10.,help="Cell latitude width at Equator in degrees",type=float)
parser.add_argument("-m", "--meridian", default=-50.,help="Reference Meridian",type=float)
parser.add_argument("-p", "--projection", default='sinu',help="WRS projection (longlat, sinu or aea",type=str)
	"""
	argswrsname = request.args.get('w', None)
	if argswrsname is None:
		return 'wrsname (w) was not defined\n'
	argsprojection = request.args.get('proj', 'aea')
	argsdegreesx = float(request.args.get('dx', 10.))
	argsdegreesy = float(request.args.get('dy', 10.))
	argsmeridian = float(request.args.get('m', 0.))

	llsrs = osr.SpatialReference()
	llsrs.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')
	if argsprojection == 'aea':
		tilesrsp4 = "+proj=aea +lat_1=10 +lat_2=-40 +lat_0=0 +lon_0=-50 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs".format(argsmeridian)
	elif argsprojection == 'sinu':
		tilesrsp4 = "+proj=sinu +lon_0={0} +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs".format(0.)
	elif argsprojection == 'longlat':
		tilesrsp4 = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
	tilesrs = osr.SpatialReference()
	tilesrs.ImportFromProj4(tilesrsp4)
	app.logger.warning('genwrs -  argsprojection {} tilesrsp4 {}'.format(argsprojection,tilesrsp4))

	ll2tile = osr.CoordinateTransformation ( llsrs, tilesrs )
	tile2ll = osr.CoordinateTransformation ( tilesrs, llsrs )

# Number of tiles and base tile
	numtilesx = int(360./argsdegreesx)
	numtilesy = int(180./argsdegreesy)
	hBase = numtilesx/2
	vBase = numtilesy/2
	app.logger.warning('genwrs - hBase {} vBase {}'.format(hBase,vBase))

# Tile size in meters (dx,dy) at center
	(x1, y, z ) = ll2tile.TransformPoint( argsmeridian - argsdegreesx/2, 0.)
	(x2, y, z ) = ll2tile.TransformPoint( argsmeridian + argsdegreesx/2, 0.)
	dx = x2-x1
	(x, y1, z ) = ll2tile.TransformPoint( argsmeridian, -argsdegreesy/2)
	(x, y2, z ) = ll2tile.TransformPoint( argsmeridian,  argsdegreesy/2)
	dy = y2-y1

# Coordinates of WRS center (top left pixel of (hBase,vBase))
	(xCenter, yCenter, z ) = ll2tile.TransformPoint( 0., 0.)

# Border coordinates of WRS grid
	xMin = xCenter - dx*hBase
	xMax = xCenter + dx*hBase
	yMin = yCenter - dy*vBase
	yMax = yCenter + dy*vBase

# Create a wrs definition in wrs table
	connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('DB_USER'),
											  os.environ.get('DB_PASS'),
											  os.environ.get('DB_HOST'),
											  'datastorm')
	engine = sqlalchemy.create_engine(connection)
	sql = "DELETE FROM wrs WHERE name = '{}'".format(argswrsname)
	result = engine.execute(sql)

# Create shapefile for this wrs in longlat
# Set up the shapefile driver
	driver = ogr.GetDriverByName("ESRI Shapefile")
# Create the data source
	shapename = '/ds_config/'+argswrsname+".shp"
	if os.path.exists(shapename):
		driver.DeleteDataSource(shapename)
	data_source = driver.CreateDataSource(shapename)
# Create the layer
	layer = data_source.CreateLayer("scene", llsrs, ogr.wkbPolygon)
# Add the fields we're interested in
	tile_name = ogr.FieldDefn("Tile", ogr.OFTString)
	tile_name.SetWidth(24)
	layer.CreateField(tile_name)
	layer.CreateField(ogr.FieldDefn("h", ogr.OFTInteger))
	layer.CreateField(ogr.FieldDefn("v", ogr.OFTInteger))

# Define the area of interest in long/lat
	w = -82.
	e = -30.
	n =  13.
	s = -56.
	# minimum and maximum x and h
	(xl, y, z ) = ll2tile.TransformPoint(w,0.)
	(xr, y, z ) = ll2tile.TransformPoint(e,0.)
	hMin = int((xl - xMin)/dx)
	hMax = int((xr - xMin)/dx)

# Minimum and maximum y and v
	(x, yu, z ) = ll2tile.TransformPoint(argsmeridian,n)
	(x, yb, z ) = ll2tile.TransformPoint(argsmeridian,s)
	vMin = int((yMax - yu)/dy)
	vMax = int((yMax - yb)/dy)
	for ix in range(hMin,hMax+1):
		x1 = xMin + ix*dx
		x2 = x1 + dx
		for iy in range(vMin,vMax+1):
			y1 = yMax - iy*dy
			y2 = y1 - dy
# Create ring for this tile
			ring = ogr.Geometry(ogr.wkbLinearRing)
			(x, y, z ) = tile2ll.TransformPoint( x1, y1)
			ring.AddPoint(x, y)
			(x, y, z ) = tile2ll.TransformPoint( x2, y1)
			ring.AddPoint(x, y)
			(x, y, z ) = tile2ll.TransformPoint( x2, y2)
			ring.AddPoint(x, y)
			(x, y, z ) = tile2ll.TransformPoint( x1, y2)
			ring.AddPoint(x, y)
			(x, y, z ) = tile2ll.TransformPoint( x1, y1)
			ring.AddPoint(x, y)
# Create polygon for the tile
			tilepoly = ogr.Geometry(ogr.wkbPolygon)
			tilepoly.AddGeometry(ring)
			(wx,ex,sy,ny) = tilepoly.GetEnvelope()
# Select LC8 scenes that intersects the mgrs bbox
			sql = "SELECT * FROM wrs WHERE name = 'WRS2'"
			sql += " AND lonmax > {}".format(wx)
			sql += " AND lonmin < {}".format(ex)
			sql += " AND latmax > {}".format(sy)
			sql += " AND latmin < {}".format(ny)
			result = engine.execute(sql)
			nscenes = result.fetchall()
			hv = '{0:03d}{1:03d}'.format(ix,iy)
			if len(nscenes) == 0:
				continue
# create the feature
			feature = ogr.Feature(layer.GetLayerDefn())
# Set the feature geometry using the polygon
			feature.SetGeometry(tilepoly)
# Set the attributes using the values from the delimited text file
			feature.SetField2("Tile", hv)
			feature.SetField2("h", ix)
			feature.SetField2("v", iy)
# Create the feature in the layer (shapefile)
			layer.CreateFeature(feature)

# Insert tile in wrs table
			scene = {}
			scene['name'] = argswrsname
			scene['path'] = iy
			scene['row'] = ix
			scene['tileid'] = hv
			scene['srs'] = tilesrsp4
			scene['geom'] = tilepoly.ExportToWkt()
			scene['lonmax'] = ex
			scene['lonmin'] = wx
			scene['latmax'] = ny
			scene['latmin'] = sy
			scene['xmax'] = x2
			scene['ymax'] = y1
			scene['xmin'] = x1
			scene['ymin'] = y2
			params = ''
			values = ''
			for key,val in scene.items():
				params += key+','
				if type(val) is str:
					values += "'{0}',".format(val)
				else:
					values += "{0},".format(val)

			sql = "INSERT INTO wrs ({0}) VALUES({1})".format(params[:-1],values[:-1])
			engine.execute(sql)

	engine.dispose()
	return 'OK\n'

def remote_file_exists(url):
	status = requests.head(url).status_code

	if status == 200:
		return True
	else:
		return False

def utmzone(lon,lat):
	if 56 <= lat < 64 and 3 <= lon < 12:
		return 32
	if 72 <= lat < 84 and 0 <= lon < 42:
		if lon < 9:
			return 31
		elif lon < 21:
			return 33
		elif lon < 33:
			return 35
		return 37
	return int((lon + 180) / 6) + 1

###################################################
@app.route('/loadWRS2', methods=['GET'])
def loadWRS2():
# Create Landsat WRS
	shapefile = "/ds_config/wrs2_descending.shp"
	if not os.path.exists(shapefile):
		app.logger.warning('loadMGRS - could not find '+shapefile)
		return 'loadWRS2 - could not find {}\n'.format(shapefile)
	driver = ogr.GetDriverByName("ESRI Shapefile")
	dataSource = driver.Open(shapefile, 0)
	layer = dataSource.GetLayer()
	llsrs = osr.SpatialReference()
	tilesrs = osr.SpatialReference()
	llsrs.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')

	scene = {}
	scene['name'] = 'WRS2'
	connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('DB_USER'),
											  os.environ.get('DB_PASS'),
											  os.environ.get('DB_HOST'),
											  'datastorm')
	engine = sqlalchemy.create_engine(connection)
	sql = "DELETE FROM wrs WHERE name = '{}'".format(scene['name'])
	result = engine.execute(sql)
	count = 0
	for feature in layer:
		path = int(feature.GetField("PATH"))
		row  = int(feature.GetField("ROW"))
		if path < 13 or path > 200:
			if row  > 50 and row < 100:
				url = 'https://landsat.usgs.gov/GCP/{0:03d}/{1:03d}/GCPLib'.format(path, row)
				if not remote_file_exists(url): 
					continue
				geom = feature.GetGeometryRef()
				xy = geom.Centroid().GetPoint(0)
				x = xy[0]
				y = xy[1]
				czone = utmzone(x,y)
				proj4 = '+proj=utm +zone={0} +datum=WGS84 +units=m +no_defs'.format(czone)
				tilesrs.ImportFromProj4(proj4)
				ll2tile = osr.CoordinateTransformation ( llsrs, tilesrs )
				bbox = geom.GetEnvelope()

				(x1, y1, z ) = ll2tile.TransformPoint( bbox[0], bbox[2])
				(x2, y2, z ) = ll2tile.TransformPoint( bbox[0], bbox[3])
				(x3, y3, z ) = ll2tile.TransformPoint( bbox[1], bbox[2])
				(x4, y4, z ) = ll2tile.TransformPoint( bbox[1], bbox[3])
				xmin = min(x1,x2,x3,x4)
				xmax = max(x1,x2,x3,x4)
				ymin = min(y1,y2,y3,y4)
				ymax = max(y1,y2,y3,y4)

# Insert tile in wrs table
				scene['path'] = path
				scene['row'] = row
				scene['tileid'] = '{0:03d}{1:03d}'.format(path,row)
				scene['srs'] = proj4
				scene['geom'] = geom.ExportToWkt()
				scene['lonmax'] = bbox[1]
				scene['lonmin'] = bbox[0]
				scene['latmax'] = bbox[3]
				scene['latmin'] = bbox[2]
				scene['xmax'] = xmax
				scene['ymax'] = ymax
				scene['xmin'] = xmin
				scene['ymin'] = ymin
				params = ''
				values = ''
				for key,val in scene.items():
					params += key+','
					if type(val) is str:
						values += "'{0}',".format(val)
					else:
						values += "{0},".format(val)

				sql = "INSERT INTO wrs ({0}) VALUES({1})".format(params[:-1],values[:-1])
				count += 1
				app.logger.warning('loadWRS2 -  scene {} {} '.format(count,scene['tileid']))
				engine.execute(sql)
				#if count == 10: break

	engine.dispose()
	return 'OK\n'

#########################################
@app.route('/checkMGRS', methods=['GET'])
def checkMGRS():
	template = '*B08.tif'
	basedir = '/Repository/Archive/S2_MSI'
	app.logger.warning('checkMGRS template - {} basedir {}'.format(template,basedir))
	tiffiles = [os.path.join(dirpath, f)
		for dirpath, dirnames, files in os.walk("{0}".format(basedir))
		for f in fnmatch.filter(files, template)]
	app.logger.warning('checkMGRS jp2files - {}'.format(len(tiffiles)))
	files = {}
	for tiffile in tiffiles:
		cc = tiffile.split('_')
		tile = cc[-4][1:]
		if len(tile) != 5: tile = cc[-3][1:]
		if tile not in files:
			files[tile]={}
		dataset = gdal.Open(tiffile,GA_ReadOnly)
		geotransform = dataset.GetGeoTransform()
		xy = '{}_{}'.format(geotransform[0],geotransform[3])
		if xy not in files[tile]:
			files[tile][xy] = 0
		files[tile][xy] += 1
		#app.logger.warning('files-{}'.format(files))
	for tile in files:
		if len(files[tile]) > 1:
			app.logger.warning('files-{}'.format(files[tile]))
	return jsonify(files)
#########################################
@app.route('/loadMGRS', methods=['GET'])
def loadMGRS():
	shapefile = "/ds_config/sentinel2_tiles_world.shp"
	if not os.path.exists(shapefile):
		app.logger.warning('loadMGRS - could not find '+shapefile)
		return 'loadMGRS - could not find '+shapefile
	"""
	template = '*B08.tif'
	basedir = '/Repository/Archive/S2_MSI'
	app.logger.warning('loadMGRS template - {} basedir {}'.format(template,basedir))
	tiffiles = [os.path.join(dirpath, f)
		for dirpath, dirnames, files in os.walk("{0}".format(basedir))
		for f in fnmatch.filter(files, template)]
	app.logger.warning('loadMGRS jp2files - {}'.format(tiffiles))
	files = {}
	for tiffile in tiffiles:
		cc = tiffile.split('_')
		tile = cc[-4][1:]
		if tile not in files:
			files[tile]=tiffile
	""" 
	driver = ogr.GetDriverByName("ESRI Shapefile")
	dataSource = driver.Open(shapefile, 0)
	layer = dataSource.GetLayer()
	count = 0
	connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('DB_USER'),
											  os.environ.get('DB_PASS'),
											  os.environ.get('DB_HOST'),
											  'datastorm')
	engine = sqlalchemy.create_engine(connection)
	scene = {}
	scene['name'] = 'MGRS'
	sql = "DELETE FROM wrs WHERE name = '{}'".format(scene['name'])
	result = engine.execute(sql)

	llsrs = osr.SpatialReference()
	llsrs.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')
	for feature in layer:
		id  = str(feature.GetField("Name"))
		#if id not in files: continue
		geom = feature.GetGeometryRef()
		(wx,ny,ex,sy) = geom.GetEnvelope()
		(wx,ex,sy,ny) = geom.GetEnvelope()
# Select LC8 scenes that intersects the mgrs bbox
		sql = "SELECT * FROM wrs WHERE name = 'WRS2'"
		sql += " AND lonmax > {}".format(wx)
		sql += " AND lonmin < {}".format(ex)
		sql += " AND latmax > {}".format(sy)
		sql += " AND latmin < {}".format(ny)
		result = engine.execute(sql)
		nscenes = result.fetchall()
		if len(nscenes) == 0: continue
		pr = ''
		for s in nscenes:
			pr += s['tileid']+ ' ' 
		app.logger.warning('id-{} wx {} ny {} ex {} sy {} pr {}'.format(id,wx,ny,ex,sy,pr))
		count += 1
		scene['path'] = -1
		scene['row'] = -1
		scene['tileid'] = id
		zone = int(id[0:2])
		proj4 = '+proj=utm +zone={0} +datum=WGS84 +units=m +no_defs'.format(zone)
		scene['srs'] = proj4
		scene['geom'] = geom.ExportToWkt()
		scene['lonmax'] = ex
		scene['lonmin'] = wx
		scene['latmax'] = ny
		scene['latmin'] = sy
		scenesrs = osr.SpatialReference()
		scenesrs.ImportFromProj4(scene['srs'])
		ll2s = osr.CoordinateTransformation ( llsrs, scenesrs )
		ring = geom.GetGeometryRef(0)
		app.logger.warning('id-{} geom {} ring {}'.format(id,scene['geom'],ring))
		p1 = ring.GetPoint(0)
		(x1, y1, z ) = ll2s.TransformPoint( p1[0], p1[1])
		x1 = round(x1,0)
		y1 = round(y1,0)
		app.logger.warning('id-{} x1 {} y1 {} p1 {}'.format(id,x1,y1,p1))
		x2 = x1 + 10980*10
		y2 = y1 - 10980*10
		scene['xmax'] = x2
		scene['ymax'] = y1
		scene['xmin'] = x1
		scene['ymin'] = y2
		app.logger.warning('id-{} x2 {} y2 {}'.format(id,x2,y2))
		"""
		dataset = gdal.Open(files[id],GA_ReadOnly)
		geotransform = dataset.GetGeoTransform()
		app.logger.warning('scene1-{}'.format(scene))
		scene['xmin'] = geotransform[0]
		scene['xmax'] = geotransform[0] + dataset.RasterXSize*geotransform[1]
		scene['ymax'] = geotransform[3]
		scene['ymin'] = geotransform[3] + dataset.RasterYSize*geotransform[5]
		app.logger.warning('id-{} geotransform {}'.format(id,geotransform))
		app.logger.warning('scene2-{}'.format(scene))
		"""
		params = ''
		values = ''
		for key,val in scene.items():
			params += key+','
			if type(val) is str:
				values += "'{0}',".format(val)
			else:
				values += "{0},".format(val)

		sql = "INSERT INTO wrs ({0}) VALUES({1})".format(params[:-1],values[:-1])
		app.logger.warning(sql)
		engine.execute(sql)
	engine.dispose()
	return 'OK'

@app.errorhandler(400)
def handle_bad_request(e):
    resp = jsonify({'code': 400, 'message': 'Bad Request - {}'.format(e.description)})
    resp.status_code = 400
    resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp


@app.errorhandler(404)
def handle_page_not_found(e):
    resp = jsonify({'code': 404, 'message': 'Page not found'})
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
