#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Command utility to download satellite scenes from Google Cloud Storage."""

# Python
import os
from pathlib import Path
from typing import Tuple
# 3rdparty
from google.cloud import storage
# Collection Builder
from .download import remove_tile_compression
from .utils import compress_landsat_scene, factory


def download_from_google(scene_id: str, destination: str, bucket_name: str = 'gcp-public-data-landsat') -> Tuple[str, str]:
    """Download files from Google Cloud Storage."""
    # Creates a GCS Client
    storage_client = storage.Client()

    scene = factory.get_from_sceneid(scene_id, level=1)

    pathrow = scene.scene_fragments[2]

    blob_name = '{}/01/{}/{}/{}'.format(scene.scene_fragments[0], pathrow[:3], pathrow[-3:], scene_id)

    # Creates the new bucket
    bucket = storage_client.bucket(bucket_name)

    blobs = list(bucket.list_blobs(prefix=blob_name))

    if len(blobs) == 0:
        raise RuntimeError('Scene {} not found on Google Cloud Storage.'.format(scene_id))

    downloaded_files = []

    for blob in blobs:
        blob_path = Path(blob.name)
        file_name = blob_path.name

        target_path = Path(destination) / file_name
        target_path.parent.mkdir(parents=True, exist_ok=True)

        blob.download_to_filename(str(target_path))

        if blob_path.suffix.lower().endswith('tif'):
            remove_tile_compression(str(target_path))

        downloaded_files.append(str(target_path))

    # Compress Downloaded scenes to tar.gz
    compressed_file = compress_landsat_scene(scene, destination)

    for temp_file in downloaded_files:
        os.remove(str(temp_file))

    return str(compressed_file), '{}/{}'.format(bucket_name, blob_name)
