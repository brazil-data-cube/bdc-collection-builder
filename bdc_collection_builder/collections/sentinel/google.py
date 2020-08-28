#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Command utility to download satellite scenes from Google Cloud Storage."""

# Python
import logging
import os
import shutil
from pathlib import Path
from typing import Tuple
# 3rdparty
from bdc_core.decorators import working_directory
from google.cloud import storage

from .utils import factory


def download_from_google(scene_id: str, destination: str, bucket_name: str = 'gcp-public-data-sentinel-2'):
    """Download files from Google Cloud Storage."""
    try:
        # Creates a GCS Client
        storage_client = storage.Client()

        scene = factory.get_from_sceneid(scene_id, level=1)

        tile_id = scene.tile_id()

        safe_folder = f'{scene_id}.SAFE'

        blob_name = Path(f'tiles/{tile_id[:2]}/{tile_id[2]}/{tile_id[3:]}/{safe_folder}')

        # Creates the new bucket
        bucket = storage_client.bucket(bucket_name)

        blobs = list(bucket.list_blobs(prefix=str(blob_name)))

        if len(blobs) == 0:
            raise RuntimeError('Scene {} not found on Google Cloud Storage.'.format(scene_id))

        downloaded_files = []

        for blob in blobs:
            blob_path = Path(blob.name)

            if blob.name.endswith(f'{safe_folder}_$folder$'):
                continue

            blob_relative = blob_path.relative_to(blob_name)

            target_path = Path(destination) / safe_folder / str(blob_relative)
            target_path.parent.mkdir(parents=True, exist_ok=True)

            if str(blob_path).endswith('$folder$'):
                continue

            blob.download_to_filename(str(target_path))

            downloaded_files.append(str(target_path))

        with working_directory(destination):
            shutil.make_archive(base_dir=safe_folder,
                                format='zip',
                                base_name=scene_id)
        return Path(destination) / f'{scene_id}.zip'
    except Exception as e:
        logging.error(f'Could not download from Google {scene_id} - {str(e)}')

        return None
