#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Handle Sentinel Download interface."""

import logging
import os
import shutil
from pathlib import Path

import requests

# 3rdparty
from bdc_catalog.models import Collection
from bdc_core.decorators import working_directory
from sentinelhub import AwsProductRequest, SHConfig

# Builder
from bdc_collection_builder.collections.utils import get_credentials
from bdc_collection_builder.config import Config


def _download(file_path: str, response: requests.Response):
    """Write compressed sentinel output to disk.

    Args:
        file_path - Path to store compressed data
        response - HTTP Response object
    """
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # May throw exception for read-only directory
    stream = open(file_path, 'wb')

    # Read chunks of 2048 bytes
    chunk_size = 2048

    for chunk in response.iter_content(chunk_size):
        stream.write(chunk)

    stream.close()


def download_sentinel_images(link, file_path, user):
    """Download sentinel image from Copernicus (compressed data).

    Args:
        link (str) - Sentinel Image Link
        file_path (str) - Path to save download file
        user (AtomicUser) - User credential
    """
    try:
        response = requests.get(link, auth=(user.username, user.password), timeout=90, stream=True)
    except requests.exceptions.ConnectionError as e:
        logging.error('Connection error during Sentinel Download')
        raise e

    if response.status_code == 202:
        raise requests.exceptions.HTTPError('Data is offline. {}'.format(response.status_code))

    if response.status_code == 401:
        raise requests.exceptions.RequestException('Invalid credentials for "{}"'.format(user.username))

    if response.status_code >= 403:
        raise requests.exceptions.HTTPError('Invalid sentinel request {}'.format(response.status_code))

    size = int(response.headers['Content-Length'].strip())

    logging.info('Downloading image {} in {}, user {}, size {} MB'.format(link, file_path, user, int(size / 1024 / 1024)))

    _download(file_path, response)


def download_sentinel_from_creodias(scene_id: str, file_path: str):
    """Download sentinel image from CREODIAS provider.

    Args:
        scene_id Sentinel scene id
        file_path Path to save sentinel
    """
    credentials = get_credentials().get('creodias')

    if credentials is None:
        raise RuntimeError('No credentials set for CREODIAS provider')

    url = 'https://auth.creodias.eu/auth/realms/DIAS/protocol/openid-connect/token'

    params = dict(
        username=credentials.get('username'),
        password=credentials.get('password'),
        client_id='CLOUDFERRO_PUBLIC',
        grant_type='password'
    )

    token_req = requests.post(url, data=params)

    if token_req.status_code != 200:
        raise RuntimeError('Unauthorized')

    token = token_req.json()

    feature_params = dict(
        maxRecords=10,
        processingLevel='LEVEL1C',
        sortParam='startDate',
        sortOrder='descending',
        status='all',
        dataset='ESA-DATASET',
        productIdentifier='%{}%'.format(scene_id)
    )
    feature_url = 'https://finder.creodias.eu/resto/api/collections/Sentinel2/search.json'
    features_response = requests.get(feature_url, params=feature_params)

    if features_response.status_code != 200:
        raise RuntimeError('Invalid request')

    features = features_response.json()

    if len(features.get('features')) > 0:
        link = 'https://zipper.creodias.eu/download/{}?token={}'.format(features['features'][0]['id'], token['access_token'])
        response = requests.get(link, timeout=90, stream=True)

        if response.status_code != 200:
            raise RuntimeError('Could not download {} - {}'.format(response.status_code, scene_id))

        _download(file_path, response)


def download_from_aws(scene_id: str, destination: str, **kwargs):
    """Download the Sentinel Scene from AWS.

    It uses the library `sentinelhub-py <https://sentinelhub-py.readthedocs.io>`_ to download
    the Sentinel-2 SAFE folder. Once downloaded, it compressed into a `zip`.

    Notes:
        Make sure to set both `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in environment variable.

        This method does not raise Exception.

    Args:
        scene_id - Sentinel-2 Product Id (We call as scene_id)
        destination - Path to store data. We recommend to use python `tempfile.TemporaryDirectory` and then move.
        collection - The collection which refer to the `scene_id`

    Returns:
        Path to the downloaded file when success or None when an error occurred.
    """
    try:
        config = SHConfig()
        config.aws_access_key_id = Config.AWS_ACCESS_KEY_ID
        config.aws_secret_access_key = Config.AWS_SECRET_ACCESS_KEY

        logging.info(f'Downloading {scene_id} From AWS...')

        request = AwsProductRequest(
            product_id=scene_id,
            data_folder=destination,
            safe_format=True,
            config=config
        )
        _ = request.get_data(save_data=True)

        file_name = '{}.SAFE'.format(scene_id)

        logging.info(f'Compressing {scene_id}.SAFE...')

        with working_directory(destination):
            shutil.make_archive(base_dir=file_name,
                                format='zip',
                                base_name=scene_id)

        return Path(destination) / file_name

    except BaseException as e:
        logging.error(f'Error downloading from AWS. {scene_id} - {str(e)}')
        return None
