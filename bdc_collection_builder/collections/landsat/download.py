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
from bdc_core.decorators.utils import working_directory
from bs4 import BeautifulSoup
from requests import Session as RequestSession
import requests
import tarfile
# Builder
from bdc_collection_builder.core.utils import get_credentials


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


def _download_file(stream: requests.Response, target: str, byte_size=4096):
    """Download request steam to file."""
    with open(target, 'wb') as fs:
        for chunk in stream.iter_content(chunk_size=byte_size):
            fs.write(chunk)


def download_from_aws(scene_id: str, destination: str, compressed_path: str = None, chunk_size: int = 512*1024):
    """Download Landsat 8 from public AWS bucket.

    After files downloaded, it compresses into SCENE_ID.tar.gz to act like USGS provider.

    Further details on https://docs.opendata.aws/landsat-pds/readme.html

    Args:
        scene_id - Lansat 8 scene id. Example: LC08_L1TP_139045_20170304_20170316_01_T1.
        destination - Path to store downloaded file.
        chunk_size - Request chunk size download. Default is 512kb.
    """
    from .publish import BAND_MAP_DN

    if compressed_path == None:
        compressed_path = destination

    os.makedirs(compressed_path, exist_ok=True)

    compressed_path = os.path.join(compressed_path, '{}.tar.gz'.format(scene_id))

    files = ['{}_{}.TIF'.format(scene_id, b) for b in BAND_MAP_DN.values()]
    files.append('{}_MTL.txt'.format(scene_id))
    files.append('{}_ANG.txt'.format(scene_id))

    pathrow = scene_id.split('_')[2]

    path, row = pathrow[:3], pathrow[3:]

    os.makedirs(destination, exist_ok=True)

    url = 'https://landsat-pds.s3.amazonaws.com/c1/L8/{}/{}/{}'.format(path, row, scene_id)

    for f in files:
        stream = requests.get('{}/{}'.format(url, os.path.basename(f)), timeout=90, stream=True)

        # Throw for any HTTP error code
        stream.raise_for_status()

        logging.debug('Downloading {}...'.format(f))
        _download_file(stream, os.path.join(destination, f), byte_size=chunk_size)

    try:
        logging.debug('Compressing {}'.format(compressed_path))
        # Create compressed file and make available
        with tarfile.open(compressed_path, 'w:gz') as compressed_file:
            with working_directory(destination):
                for f in files:
                    compressed_file.add(f)

    except BaseException:
        logging.error('Could not compress {}.tar.gz'.format(scene_id), exc_info=True)

        raise

    return compressed_path


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
