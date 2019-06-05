from urllib.parse import urlencode
from flask import jsonify
from catalog import utils
import requests
import logging
import json
import os
# from osgeo import ogr


def setProviders():
	# In case of change in provider TYPE,
	# changes must be made to the file "ms3_search.py".
	with open('catalog/config/providers.json') as f:
		data = json.load(f)
		for key, value in data.items():
			os.environ[key] = value['urlr']
		return data
	return ''


def opensearch(provider, url, params):
	logging.warning('ms3_search opensearch - provider {} url {}'.format(provider,url))
	logging.warning('ms3_search opensearch - provider {} params {}'.format(provider,urlencode(params)))
	logging.warning('ms3_search opensearch - provider {} {}'.format(provider,url + 'granule.json?&' + urlencode(params)))
	try:
		response = requests.get(url + 'granule.json?&' + urlencode(params))
	except requests.exceptions.RequestException as exc:
		logging.warning('ms3_search opensearch - Error {}'.format(exc))
		resp = jsonify({'code': 500, 'message': 'Internal Server Error'})
		resp.status_code = 500
		resp.headers.add('Access-Control-Allow-Origin', '*')
		return resp

	response = json.loads(response.text)
	response['provider'] = provider
	# TODO: validate quicklook or images download url
	return response

def cubesearch(provider, url, params):
	logging.warning('ms3_search cubesearch - {}'.format(url + 'granule.json?&' + urlencode(params)))
	try:
		response = requests.get(url + 'granule.json?&' + urlencode(params))
	except requests.exceptions.RequestException as exc:
		logging.warning('ms3_search cubesearch - Error {}'.format(exc))
		resp = jsonify({'code': 500, 'message': 'Internal Server Error'})
		resp.status_code = 500
		resp.headers.add('Access-Control-Allow-Origin', '*')
		return resp

	response = json.loads(response.text)
	logging.warning('ms3_search cubesearch - response {}'.format(response))
	response['provider'] = provider
	# TODO: validate quicklook or images download url
	return response


def search(params):
	logging.warning('search - {}'.format(urlencode(params)))
	results = dict()
	providers = json.loads(params['providers'])
	for key, value in providers.items():
		logging.warning('search - key {} - value {}'.format(key,value))
		if value['type'] == 'cubesearch':
			logging.warning('search - cubesearch - {}'.format(urlencode(params)))
			results[key] = cubesearch(key, value['url'], params)
		elif value['type'] == 'opensearch':
			logging.warning('search - opensearch - {}'.format(urlencode(params)))
			results[key] = opensearch(key, value['urlr'], params)

	features = list()
	providers = list()
	for key, value in results.items():
		featlist = value['features']
		for feature in featlist:
			feature['properties']['provider'] = key
		features += featlist
		value.pop('features')
		providers.append(value)

	response = dict()
	response['features'] = features
	response['providers'] = providers
	return jsonify(response)
