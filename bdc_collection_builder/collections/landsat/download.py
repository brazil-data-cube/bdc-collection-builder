#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Handle Landsat 8 download interface."""


# Python Native
import logging
import os
# 3rdparty
from bs4 import BeautifulSoup
from requests import Session as RequestSession
# Builder
from bdc_collection_builder.collections.utils import get_credentials


def get_session() -> RequestSession:
    """Create a session with USGS channel.

    TODO: Use development seed STAC instead
    """
    url_login = 'https://ers.cr.usgs.gov/login/'
    session = RequestSession()
    login_html = session.get(url_login)

    html = BeautifulSoup(login_html.content, "html.parser")

    __ncforminfo = html.find("input", {"name": "__ncforminfo"}).attrs.get("value")
    csrf_token = html.find("input", {"id": "csrf_token"}).attrs.get("value")

    auth = {"username": user['username'], "password": user['password'], "csrf_token": csrf_token, "__ncforminfo": __ncforminfo}

    session.post(url_login, data=auth, allow_redirects=False)

    return session


user = get_credentials()['landsat']


def download_landsat_images(link, destination):
    """Download landsat from USGS."""
    session = get_session()

    req = session.get(link, timeout=90, stream=True)
    logging.warning('downloadLC8 - r {}'.format(req.headers))
    count = 0
    while req.headers.get("Content-Disposition") is None and count < 2:
        logging.warning('downloadLC8 - Content-Disposition not found for {}'.format(link))
        count += 1
        cc = link.split('/')
        sid = cc[-3]
        last = ord(sid[-1])+1
        last = chr(last)
        cc[-3] = sid[:-1]+last
        link = '/'.join(cc)
        req = session.get(link, stream=True)

    if count == 2:
        raise RuntimeError('Error in landsat download {} - {}'.format(link, req.status_code))

    outtar = os.path.join(destination, req.headers.get("Content-Disposition").split('=')[1])
    logging.warning('downloadLC8 - outtar {}'.format(outtar))

    if req.headers.get("Content-length") is None:
        logging.warning('downloadLC8 - Content-Length not found for {}'.format(link))
        raise RuntimeError('Error in landsat download - Content Length 0 {} - {}'.format(link, req.status_code))

    total_size = int(req.headers.get("Content-length"))
    logging.warning( 'downloadLC8 - {} to {} size {}'.format(link,outtar,int(total_size/1024/1024)))
    file_size = 0
    if os.path.exists(outtar):
        file_size = os.path.getsize(outtar)
        logging.warning( 'downloadLC8 - {} to {} link_size {} file_size {}'.format(link,outtar,total_size,file_size))
    if total_size == file_size:
        logging.warning( 'downloadLC8 - {} already downloaded'.format(link))
        return outtar

    block_size = 1024*10

    with open(outtar, 'wb') as fs:
        for chunk in req.iter_content(chunk_size=block_size):
            fs.write(chunk)

    return outtar
