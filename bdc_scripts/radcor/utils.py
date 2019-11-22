# Python Native
import datetime
import json
import logging

# 3rdparty
from celery import chain, current_task
import requests

# BDC Scripts
from bdc_scripts.radcor.models import RadcorActivityHistory
from bdc_scripts.radcor.sentinel.clients import sentinel_clients


def dispatch(activity: dict):
    from bdc_scripts.radcor.sentinel import tasks as sentinel_tasks
    from bdc_scripts.radcor.landsat import tasks as landsat_tasks

    """
    Dispatches the activity to the respective celery task handler

    Args:
        activity (RadcorActivity) - A not done activity
    """

    app = activity.get('app')

    if app == 'downloadS2':
        # TODO: Add marshmallow-sqlalchemy to enable serialization
        task_chain = sentinel_tasks.download_sentinel.s(activity) | \
                        sentinel_tasks.atm_correction.s() | \
                        sentinel_tasks.publish_sentinel.s() | \
                        sentinel_tasks.upload_sentinel.s()
        return chain(task_chain).apply_async()
    elif app == 'correctionS2':
        task_chain = sentinel_tasks.atm_correction.s(activity) | \
                        sentinel_tasks.publish_sentinel.s() | \
                        sentinel_tasks.upload_sentinel.s()
        return chain(task_chain).apply_async()
    elif app == 'publishS2':
        task_chain = sentinel_tasks.publish_sentinel.s(activity) | sentinel_tasks.upload_sentinel.s()
        return chain(task_chain).apply_async()
    elif app == 'downloadLC8':
        task_chain = landsat_tasks.download_landsat.s(activity) | \
                        landsat_tasks.amt_correction_landsat.s() | \
                        landsat_tasks.publish_landsat.s() | \
                        landsat_tasks.upload_landsat.s()
        return chain(task_chain).apply_async()
    elif app == 'correctionLC8':
        task_chain = landsat_tasks.amt_correction_landsat.s(activity) | \
                        landsat_tasks.publish_landsat.s() | \
                        landsat_tasks.upload_landsat.s()
        return chain(task_chain).apply_async()
    elif app == 'publishLC8':
        task_chain = landsat_tasks.publish_landsat.s(activity) | landsat_tasks.upload_landsat.s()
        return chain(task_chain).apply_async()
    else:
        raise ValueError('Not implemented. "{}"'.format(app))


def get_task_activity() -> RadcorActivityHistory:
    task_id = current_task.request.id

    return RadcorActivityHistory.get_by_task_id(task_id)


def create_wkt(ullon, ullat, lrlon, lrlat):
    from ogr import Geometry, wkbLinearRing, wkbPolygon

    # Create ring
    ring = Geometry(wkbLinearRing)
    ring.AddPoint(ullon, ullat)
    ring.AddPoint(lrlon, ullat)
    ring.AddPoint(lrlon, lrlat)
    ring.AddPoint(ullon, lrlat)
    ring.AddPoint(ullon, ullat)

    # Create polygon
    poly = Geometry(wkbPolygon)
    poly.AddGeometry(ring)

    return poly.ExportToWkt(),poly


def get_landsat_scenes(wlon, nlat, elon, slat, startdate, enddate, cloud, limit):
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
    r = requests.post(url, data= json.dumps(params))
    r_dict = r.json()

    scenes = {}
    # Check if request obtained results
    if r_dict['meta']['returned'] > 0:
        for i in range(len(r_dict['features'])):
            # This is performed due to BAD catalog, which includes box from -170 to +175 (instead of -)
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


def get_sentinel_scenes(wlon,nlat,elon,slat,startdate,enddate,cloud,limit,productType=None):

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
        pfootprintWkt,footprintPoly = create_wkt(wlon-0.01,nlat+0.01,elon+0.01,slat-0.01)
        pquery += ' AND (footprint:"Contains({})")'.format(footprintPoly)
    else:
        pfootprintWkt,footprintPoly = create_wkt(wlon,nlat,elon,slat)
        pquery += ' AND (footprint:"Intersects({})")'.format(footprintPoly)

    limit = int(limit)
    rows = min(100,limit)
    count_results = 0

    # Get available sentinel user.
    # TODO: Use distributed lock with redis
    user = sentinel_clients.use()

    while count_results < min(limit,totres) and totres != 0:
        rows = min(100,limit-len(scenes),totres)
        first = count_results
        query = pquery + '&rows={}&start={}'.format(rows,first)
        try:
            # Using sentinel user and release on out of scope
            with user:
                r = requests.get(query, auth=(user.username, user.password), verify=True)

            if not r.status_code // 100 == 2:
                logging.exception('openSearchS2SAFE API returned unexpected response {}:'.format(r.status_code))
                return {}
            r_dict = r.json()
            #logging.warning('r_dict - {}'.format(json.dumps(r_dict, indent=2)))

        except requests.exceptions.RequestException as exc:
            return {}

        if 'entry' in r_dict['feed']:
            totres = int(r_dict['feed']['opensearch:totalResults'])
            logging.warning('Results for this feed: {}'.format(totres))
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
                    logging.warning('openSearchS2SAFE skipping {}'.format(identifier))
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
                    logging.warning( 'openSearchS2SAFE identifier - {} - tileid {} was not found'.format(identifier,scenes[identifier]['pathrow']))
                    logging.warning(json.dumps(scenes[identifier], indent=4))
                scenes[identifier]['link'] = result['link'][0]['href']
                scenes[identifier]['icon'] = result['link'][2]['href']
        else:
            totres = 0
    return scenes
