import sys
import os
import numpy
import time
import datetime
import json
from osgeo import gdal
from osgeo import osr
from osgeo import ogr
from osgeo.gdalconst import *
import requests

#########################################
def myprint(*args):
	buf = 'Datastorm - '
	for arg in args:
		buf += '{0} '.format(arg)
	print buf+'\n',

################################
def remote_file_exists(url):
		status = requests.head(url).status_code
		if status == 200:
			return True
		else:
			return False

#################################
class Datastorm:
	def __init__(self, host, datacube, bands, verbose=2):
		self.host = host
		self.datacube = datacube
		self.bands = bands.split(',')
		self.verbose = verbose
		self.result = {}
		gdal.UseExceptions() #Exceptions will get raised on anything >= gdal.CE_Failure

################################
	def save(self):
		driver = gdal.GetDriverByName('GTiff')
		i = 0
		for identifier in self.result:
			for band in self.bands:
				filename = '{}_{}.tif'.format(self.result[identifier]['id'],band)
				if self.verbose >= 0: myprint( 'save - ',filename)
				dataset = driver.Create( filename, self.numcol, self.numlin, 1, gdal.GDT_UInt16,  options = [ 'COMPRESS=LZW' ] )
				dataset.SetGeoTransform( [self.w, self.resolution, 0, self.n, 0, -self.resolution])
				dataset.SetProjection ( self.scenesrs.ExportToWkt() )
				dataset.GetRasterBand(1).SetNoDataValue(0)
				dataset.GetRasterBand(1).WriteArray( self.rasters[i] )
				i += 1
				dataset = None

################################
	def load(self,resolution,proj4='+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'):
		self.numcol = int((self.e - self.w)/resolution)
		self.numlin = int((self.n - self.s)/resolution)
		if self.verbose >= 0: myprint( 'load - ',self.numcol,self.numlin)
		self.scenesrs = osr.SpatialReference()
		self.scenesrs.ImportFromProj4(proj4)
		self.resolution = resolution
		self.rasters = []

		for identifier in self.result:
			for band in self.bands:
				filename = '/vsicurl/'+ self.result[identifier]['links'][band]
				if self.verbose >= 0: myprint( 'load - ',filename)
				try:
					src_ds = gdal.Open(filename)
				except Exception as e:
					myprint ('Operation raised an exception',e)
					return 1,'Error1 opening {}'.format(filename)

				if src_ds is None:
					return 1,'Error2 opening {}'.format(filename)

# Now, we create an in-memory raster
				mem_drv = gdal.GetDriverByName( 'MEM' )
				tmp_ds = mem_drv.Create('', self.numcol, self.numlin, 1, gdal.GDT_UInt16)

# Set the geotransform
				tmp_ds.SetGeoTransform([self.w, self.resolution, 0, self.n, 0, -self.resolution])
				tmp_ds.SetProjection ( self.scenesrs.ExportToWkt() )

# Perform the projection/resampling
				resampling = gdal.GRA_Bilinear
				error_threshold = 0.125
				res = gdal.ReprojectImage( src_ds, tmp_ds, src_ds.GetProjection(), tmp_ds.GetProjection(), resampling)
				self.rasters.append(tmp_ds.GetRasterBand(1).ReadAsArray(0, 0, tmp_ds.RasterXSize, tmp_ds.RasterYSize))
		return self.rasters

################################
	def search(self,w,s,e,n,startdate,enddate,type,rp,limit):
		self.w = w
		self.s = s
		self.e = e
		self.n = n
		self.startdate = startdate
		self.enddate = enddate
		
		bquery = '&bbox={0},{1},{2},{3}'.format(w,s,e,n)
		bquery += '&radiometricProcessing={0}'.format(rp)
		bquery += '&start={0}'.format(startdate)
		bquery += '&end={0}'.format(enddate)
		if type is not None:
			bquery += '&type={0}'.format(type)
		bquery += '&count={0}'.format(limit)
		satsens = self.datacube.split(',')
		for satsen in satsens:
			query = '{}/granule.json?dataset={}{}'.format(self.host,satsen,bquery)
			if self.verbose > 0: myprint(query)
			r = requests.get(query)
			if r.status_code != 200:
				if self.verbose >= 0: myprint( 'search - status code',r.status_code)
			else:
				r_dict = json.loads(r.text)
				if self.verbose > 0: myprint( 'search totalResults',r_dict['totalResults'])
				if self.verbose > 2: myprint( json.dumps(r_dict, indent=4))

				for k in r_dict['features']:
					if k['type'] == 'Feature':
						identifier = k['properties']['title']
						self.result[identifier] = {}
						self.result[identifier]['id'] = identifier
						if 'Dataset' in k['properties']:
							self.result[identifier]['dataset'] = k['properties']['Dataset']
						if 'datacube' in k['properties']:
							self.result[identifier]['dataset'] = k['properties']['datacube']
						if 'date' in k['properties']:
							self.result[identifier]['date'] = k['properties']['date'][0:10]
						else:
							self.result[identifier]['date'] = k['properties']['start']
						if 'path' in k['properties']:
							self.result[identifier]['path'] = spath = k['properties']['path']
							self.result[identifier]['row']  = srow  = k['properties']['row']
							self.result[identifier]['pathrow'] = '{0:03d}{1:03d}'.format(int(k['properties']['path']),int(k['properties']['row']))
						else:
							self.result[identifier]['pathrow'] = k['properties']['tileid']
						if 'cloudcoverq1' in k['properties']:
							self.result[identifier]['cloud'] = (int(k['properties']['cloudcoverq1'])+int(k['properties']['cloudcoverq2'])+int(k['properties']['cloudcoverq3'])+int(k['properties']['cloudcoverq4']))/4
						else:
							self.result[identifier]['cloud'] = k['properties']['cloud']
						self.result[identifier]['links'] = {}
						self.result[identifier]['links'] = {}
						self.result[identifier]['files'] = {}
# Get file names
						links = k['properties']['enclosure']
						ok = True
						for link in links:
							band = link['band']
							if band not in self.bands: 
								continue
							self.result[identifier]['links'][band] = link['url']

							if not remote_file_exists(link['url']):
								myprint ('Removing ',identifier,'because',link['url'],'does not exist')
								del self.result[identifier]
								ok = False
								break
							filename = '/vsicurl/'+ link['url']
							if self.verbose >= 0: myprint( 'search checking - ',link['url'])
							try:
								src_ds = gdal.Open(filename)
							except Exception as e:
								myprint ('Removing ',identifier,'because',e)
								del self.result[identifier]
								ok = False
								break
						if ok and self.verbose > 1: myprint( 'search - scene',identifier,'contains',len(self.result[identifier]['links']),'bands')
		for identifier in self.result:
			if self.verbose > 1: myprint( 'search - ',identifier,'cloud',self.result[identifier]['cloud'])
