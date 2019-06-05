import sqlalchemy
import logging
import os

handler = logging.FileHandler('errors.log')
handler.setFormatter(logging.Formatter(
	'[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
))


logger = logging.getLogger('cubesearch')
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


def get_bbox(bbox=None, uid=None, path=None, row=None, time_start=None, time_end=None,
			 radiometric=None, image_type=None, band=None, dataset=None, start=0, count=10):
	sql = "SELECT p.* FROM products AS p WHERE "

	where = list('1')

	if path is not None and path != "" and row is not None and row != "":
		pathrow = '{0:03}{1:03}'.format(int(path),int(row))
		where.append("p.`tileid` = '{}'".format(pathrow))

	elif path is not None and path != "":
		pathrow = '{0:03}%%'.format(int(path))
		where.append("p.`tileid` LIKE '{}'".format(pathrow))

	elif row is not None and row != "":
		pathrow = '%%{0:03}'.format(int(row))
		where.append("p.`tileid` LIKE '{}'".format(pathrow))

	elif bbox is not None and bbox != "":
		try:
			for x in bbox.split(','):
				float(x)
			min_x, min_y, max_x, max_y = bbox.split(',')

			bbox = ""
			bbox += "(({} <= `TR_Longitude` and {} <=`TR_Latitude`)".format(min_x, min_y)
			bbox += " or "
			bbox +=  "({} <= `BR_Longitude` and {} <=`TL_Latitude`))".format(min_x, min_y)
			bbox += " and "
			bbox += "(({} >= `BL_Longitude` and {} >=`BL_Latitude`)".format(max_x, max_y)
			bbox += " or "
			bbox +=  "({} >= `TL_Longitude` and {} >=`BR_Latitude`))".format(max_x, max_y)

			where.append("(" + bbox + ")")

		except:
			raise (InvalidBoundingBoxError())

	if time_start is not None and time_start != "":

		where.append("p.`end` >= '{}'".format(time_start))

	if time_end is not None and time_end != "":
		where.append("p.`start`<= '{}'".format(time_end))

	if image_type is not None and image_type != "":
		where.append("p.`type` = '{}'".format(image_type))
	if band is not None and band != "":
		where.append("p.`band` = '{}'".format(band))
	if dataset is not None and dataset != "":
		where.append("p.`datacube` = '{}'".format(dataset))

	where = " and ".join(where)

	sql += where

	sql += " GROUP BY p.`sceneid` ORDER BY `start` DESC"

	sql += " LIMIT {},{}".format(start, count)

	result = do_query(sql)

	logging.warning('get_bbox - {}'.format(sql))
	sql = "SELECT COUNT(*) as len FROM (" \
		  "SELECT p.sceneid " \
		  "FROM products AS p " \
		  "WHERE {} GROUP BY p.sceneid) as p".format(where)

	result_len = do_query(sql)
	result_len = int(result_len[0]['len'])
	if result_len > 0:
		if result_len < count:
			count = result_len
	else:
		count = 0

	return make_geojson(result, result_len)


def get_updated():
	sql = "SELECT DATE_FORMAT(`update_time`,'%%Y-%%m-%%dT%%H:%%i:%%s') as `Date` " \
		  "FROM information_schema.tables WHERE table_name = 'Scene'"

	result = do_query(sql)
	return result[0]['Date']


def get_products(scene_id):
	sql = "SELECT * FROM `products` WHERE `sceneid` = '{}'".format(scene_id)
	result = do_query(sql)
	return result


def get_datasets():
	sql = "SELECT DISTINCT `datacube` as 'Dataset' FROM `datacubes`"
	result = do_query(sql)
	logging.warning('get_datasets - {}'.format(result))
	return result

def get_bands():
	sql = "SELECT DISTINCT `band` FROM `products`"
	result = do_query(sql)
	return result

def get_radiometricProcessing():
	return [{'RadiometricProcessing': 'SR'}]

def get_types():
	sql = "SELECT DISTINCT `type` as 'Type' FROM `products`"
	result = do_query(sql)
	return result

def make_geojson(data, totalResults, output='json'):
	geojson = dict()
	geojson['totalResults'] = totalResults
	geojson['type'] = 'FeatureCollection'
	geojson['features'] = []
	base_url = os.environ.get('BASE_URL')
	for i in data:
		feature = dict()
		feature['type'] = 'Feature'

		geometry = dict()
		geometry['type'] = 'Polygon'
		geometry['coordinates'] = [
		  [[i['TL_Longitude'], i['TL_Latitude']],
		   [i['BL_Longitude'], i['BL_Latitude']],
		   [i['BR_Longitude'], i['BR_Latitude']],
		   [i['TR_Longitude'], i['TR_Latitude']],
		   [i['TL_Longitude'], i['TL_Latitude']]]
		]

		feature['geometry'] = geometry
		properties = dict()
		properties['title'] = i['sceneid']
		properties['id'] = '{}/granule.{}?uid={}'.format(base_url, output, i['sceneid'])
		properties['updated'] = i['processingdate']
		properties['alternate'] = '{}/granule.{}?uid={}'.format(base_url, output, i['sceneid'])
		properties['icon'] = get_browse_image(i['sceneid'])
		properties['via'] = '{}/metadata/{}'.format(base_url, i['sceneid'])

		for key, value in i.items():
			logging.warning('make_geojson - key {} value {}'.format(key,value))
			if key == 'start' or key == 'end':
				properties[key.lower()] = "'{0}'".format(value)
			elif key != 'sceneid' and key != 'processingdate':
				properties[key.lower()] = value

		products = get_products(i['sceneid'])

		properties['enclosure'] = []
		for p in products:
			enclosure = dict()

			enclosure['band'] = p['band']
			enclosure['type'] = p['type']
			enclosure['url'] = os.environ.get('ENCLOSURE_BASE') + p['filename']
			properties['enclosure'].append(enclosure)

		feature['properties'] = properties
		geojson['features'].append(feature)

	logging.warning('make_geojson - geojson {}'.format(geojson))
	return geojson


def get_browse_image(sceneid):
	table = ''

	sql = "SELECT `qlookfile` FROM `qlook` WHERE `sceneid` = '{}'".format(sceneid)

	result = do_query(sql)
	if len(result) > 0:
		return  os.environ.get('ENCLOSURE_BASE') + result[0]['qlookfile']
	else:
		return None


def do_query(sql):
	connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('DB_USER'),
											  os.environ.get('DB_PASS'),
											  os.environ.get('DB_HOST'),
											  os.environ.get('DB_NAME'))
	engine = sqlalchemy.create_engine(connection)
	result = engine.execute(sql)
	result = result.fetchall()
	engine.dispose()
	return [dict(row) for row in result]


class InvalidBoundingBoxError(Exception):
	pass
