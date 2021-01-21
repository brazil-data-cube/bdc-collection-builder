#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Module to generate collection bands dynamically using bdc.bands.metadata property."""

import logging
from pathlib import Path
from typing import Dict, List

import numpy
import rasterio
from bdc_catalog.models import Band, Collection

from ..interpreter import execute_expression
from .utils import generate_cogs

BandMapFile = Dict[str, str]
"""Type which a key (represented as collection band name) points to generated file in disk."""


class AutoCloseDataSet:
    """Class to wraps the rasterio.io.Dataset to auto close data set out of scope."""

    def __init__(self, file_path: str, mode='r', **options):
        """Build an auto close dataset instance."""
        self.dataset = rasterio.open(str(file_path), mode=mode, **options)
        self.profile = options

    def close(self):
        """Try to close a data set."""
        if self.dataset:
            self.dataset.close()

    def __del__(self):
        """Destructor method that close datasets before object is destroyed."""
        self.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close data set when object out of scope."""
        self.close()


def generate_band_indexes(scene_id: str, collection: Collection, scenes: dict) -> BandMapFile:
    """Generate Collection custom bands based in string-expression on table `band_indexes`.

    This method seeks for custom bands on Collection Band definition. A custom band must have
    `metadata` property filled out according the ``bdc_catalog.jsonschemas.band-metadata.json``.

    Notes:
        When collection does not have any index band, returns empty dict.

    Raises:
        RuntimeError when an error occurs while interpreting the band expression in Python Virtual Machine.

    Returns:
        A dict values with generated bands.
    """
    collection_band_indexes: List[Band] = []

    for band in collection.bands:
        if band._metadata and band._metadata.get('expression') and band._metadata['expression'].get('value'):
            collection_band_indexes.append(band)

    if not collection_band_indexes:
        return dict()

    map_data_set_context = dict()
    profile = None
    blocks = []

    base_path = None

    for band_name, file_path in scenes.items():
        map_data_set_context[band_name] = AutoCloseDataSet(str(file_path), mode='r')

        if profile is None:
            profile = map_data_set_context[band_name].dataset.profile.copy()
            blocks = list(map_data_set_context[band_name].dataset.block_windows())

            base_path = Path(file_path).parent

    output = dict()

    for band_index in collection_band_indexes:
        band_name = band_index.name

        custom_band_path = base_path / f'{scene_id}_{band_name}.tif'

        try:
            band_expression = band_index._metadata['expression']['value']

            band_data_type = band_index.data_type

            data_type_info = numpy.iinfo(band_data_type)

            data_type_max_value = data_type_info.max
            data_type_min_value = data_type_info.min

            profile['dtype'] = band_data_type

            output_dataset = AutoCloseDataSet(str(custom_band_path), mode='w', **profile)

            logging.info(f'Generating band {band_name} for collection {collection.name}...')

            for _, window in blocks:
                machine_context = {
                    # TODO: Should we multiply by scale before pass to the Python Machine?
                    k: ds.dataset.read(1, masked=True, window=window).astype(numpy.float32)
                    for k, ds in map_data_set_context.items()
                }

                expr = f'{band_name} = {band_expression}'

                result = execute_expression(expr, context=machine_context)
                raster = result[band_name]
                raster[raster == numpy.ma.masked] = profile['nodata']
                # Persist the expected band data type to cast value safelly.
                raster[raster < data_type_min_value] = data_type_min_value
                raster[raster > data_type_max_value] = data_type_max_value

                output_dataset.dataset.write(raster.astype(band_data_type), window=window, indexes=1)

            output_dataset.close()

            generate_cogs(str(custom_band_path), str(custom_band_path))

            output[band_name] = str(custom_band_path)
        except Exception as e:
            logging.warning(f'Could not generate band {band_name} due {str(e)}')

            if custom_band_path.exists():
                custom_band_path.unlink()

    return output
