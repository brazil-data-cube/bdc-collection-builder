from flask import Flask, request, make_response, render_template, abort, jsonify, send_file
import inpe_data
import os
import io
import logging


app = Flask(__name__)
app.config['PROPAGATE_EXCEPTIONS'] = True
app.logger_name = "opensearch"
handler = logging.FileHandler('errors.log')
handler.setFormatter(logging.Formatter(
    '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
))

app.logger.addHandler(handler)
app.jinja_env.trim_blocks = True
app.jinja_env.lstrip_blocks = True
app.jinja_env.keep_trailing_newline = True


@app.route('/granule.<string:output>', methods=['GET'])
def os_granule(output):
    data = []
    total_results = 0

    start_index = request.args.get('startIndex', 1)
    count = request.args.get('count', 10)

    if start_index == "":
        start_index = 0
    elif int(start_index) == 0:
        abort(400, 'Invalid startIndex')
    else:
        start_index = int(start_index) - 1
    if count == "":
        count = 10
    elif int(count) < 0:
        abort(400, 'Invalid count')
    else:
        count = int(count)

    try:
        data = inpe_data.get_bbox(request.args.get('bbox', None),
                                  request.args.get('uid', None),
                                  request.args.get('path', None),
                                  request.args.get('row', None),
                                  request.args.get('start', None),
                                  request.args.get('end', None),
                                  request.args.get('radiometricProcessing', None),
                                  request.args.get('type', None),
                                  request.args.get('band', None),
                                  request.args.get('dataset', None),
                                  request.args.get('cloud', None),
                                  start_index, count)
    except inpe_data.InvalidBoundingBoxError:
        abort(400, 'Invalid bounding box')
    except IOError:
        abort(503)


    if output == 'json':
        resp = jsonify(data)
        resp.headers.add('Access-Control-Allow-Origin', '*')
        return resp

    resp = make_response(render_template('granule.{}'.format(output),
                                         url=request.url.replace('&', '&amp;'),
                                         data=data, start_index=start_index, count=count,
                                         url_root=os.environ.get('BASE_URL')))

    if output == 'atom':
        resp.content_type = 'application/atom+xml' + output

    resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp


@app.route('/collections.<string:output>')
def os_dataset(output):
    abort(503) # disabled at the moment

    total_results = 0
    data = None
    start_index = request.args.get('startIndex', 1)
    count = request.args.get('count', 10)

    if start_index == "":
        start_index = 0
    elif int(start_index) == 0:
        abort(400, 'Invalid startIndex')

    else:
        start_index = int(start_index) - 1

    if count == "":
        count = 10
    elif int(count) < 0:
        abort(400, 'Invalid count')
    else:
        count = int(count)

    try:
        result = inpe_data.get_datasets(request.args.get('bbox', None),
                                        request.args.get('searchTerms', None),
                                        request.args.get('uid', None),
                                        request.args.get('start', None),
                                        request.args.get('end', None),
                                        start_index, count)

        data = result
    except IOError:
        abort(503)

    resp = make_response(render_template('collections.' + output,
                                         url=request.url.replace('&', '&amp;'),
                                         data=data, total_results=len(result),
                                         start_index=start_index, count=count,
                                         url_root=request.url_root,
                                         updated=inpe_data.get_updated()
                                         ))
    if output == 'atom':
        output = 'atom+xml'
    resp.content_type = 'application/' + output
    resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp


@app.route('/')
@app.route('/osdd')
@app.route('/osdd/granule')
def os_osdd_granule():
    resp = make_response(render_template('osdd_granule.xml',
                                         url=os.environ.get('BASE_URL'),
                                         datasets=inpe_data.get_datasets(),
                                         bands=inpe_data.get_bands(),
                                         rps=inpe_data.get_radiometricProcessing(),
                                         types=inpe_data.get_types()))
    resp.content_type = 'application/xml'
    resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp


@app.route('/osdd/collection')
def os_osdd_collection():
    resp = make_response(render_template('osdd_collection.xml', url=request.url_root))
    resp.content_type = 'application/xml'
    resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp


@app.route('/browseimage/<string:sceneid>')
def browse_image(sceneid):
    try:
        image = inpe_data.get_browse_image(sceneid)
    except IndexError:
        abort(400, 'There is no browse image with the provided Scene ID.')
    except Exception as e:
        abort(503, str(e))

    return send_file(io.BytesIO(image), mimetype='image/jpeg')


@app.route('/metadata/<string:sceneid>')
def scene(sceneid):
    try:
        data, result_len = inpe_data.get_bbox(uid=sceneid)
        data[0]['browseURL'] = request.url_root + data[0]['browseURL']
    except Exception as e:
        abort(503, str(e))

    return jsonify(data)

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
