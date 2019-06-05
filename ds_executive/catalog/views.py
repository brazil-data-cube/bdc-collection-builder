from flask import render_template, request
from catalog import app
from catalog import ms3_search
from catalog import utils
import os

@app.before_request
def before_request():
    if 'localhost' in request.host_url or '0.0.0.0' in request.host_url:
        app.jinja_env.cache = {}

@app.route("/")
def index():

    return render_template("index.jinja2", host_url=os.getenv("HOST_URL"), providers=ms3_search.setProviders(),
    wrs=utils.get_wrs_geojson())

@app.route("/query", methods=['GET'])
def query():
    return ms3_search.search(request.args.to_dict())

@app.errorhandler(500)
def exception_handler(exception):
    app.logger.exception(exception)
    return """<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
            <title>500 Internal Server Error</title>
            <h1>Internal Server Error</h1>
            <p>The server encountered an internal error and was 
            unable to complete your request..</p>"""
