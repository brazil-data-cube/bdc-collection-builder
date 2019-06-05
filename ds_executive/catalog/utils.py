from urllib.parse import urlencode
from flask import jsonify
from catalog import utils
import requests
import json
import os
import sqlalchemy
from geomet import wkt

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

def get_wrs_geojson():
    wrs = dict()
    sql = f"SELECT name, geom FROM `wrs` LIMIT 10000"
    
    result = do_query(sql)
    
    geojson = dict()    
    geojson['type'] = 'FeatureCollection'
    geojson['features'] = []	
    for r in result:
        geometry = dict()

        try:
            feature = dict()
            feature['type'] = 'Feature'
            feature['geometry'] = wkt.loads(r["geom"])
            feature['properties'] = {'wrs':r["name"]}   
            geojson['features'].append(feature)
        except ValueError as e:
            pass
            
        wrs[r["name"]] = geojson   
    return wrs


def make_geojson(features, totalResults, provider):
    geojson = dict()
    geojson['totalResults'] = totalResults
    geojson['provider'] = provider
    geojson['type'] = 'FeatureCollection'
    geojson['features'] = []
    for i in features:
        geojson['features'].append(i)

    return geojson


def remote_file_exists(url):
    status = requests.head(url).status_code
    if status == 200:
        return True
    else:
        return False
