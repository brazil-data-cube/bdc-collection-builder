#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Define the Collection Builder utilities for Landsat data products."""

import logging
import tarfile
from datetime import datetime
from pathlib import Path
from typing import Union

from bdc_catalog.models import Collection
from bdc_core.decorators.utils import working_directory

from ...config import Config


class LandsatProduct:
    """Define base class for handling Landsat data products."""

    collection: Collection
    band_map: dict = None

    def __init__(self, scene_id: str, collection: Collection):
        """Build a Landsat class."""
        self.scene_id = scene_id
        self._fragments = LandsatProduct.parse_scene_id(scene_id)

        if not isinstance(collection, Collection):
            raise RuntimeError(f'The attribute "collection" ({collection}) must be a {Collection}')

        self.collection = collection

        self.get_band_map()

    @property
    def scene_fragments(self):
        if self._fragments is None:
            self._fragments = LandsatProduct.parse_scene_id(self.scene_id)
        return self._fragments

    @staticmethod
    def parse_scene_id(scene_id: str):
        """Parse a Landsat Scene Identifier."""
        fragments = scene_id.split('_')

        if len(fragments) != 7:
            raise ValueError('Invalid scene id Landsat')

        return fragments

    @property
    def id(self) -> str:
        """Retrieve Landsat Collection ID on Brazil Data Cube."""
        return self.collection.name

    @property
    def level(self) -> int:
        """Retrieve Landsat Collection Level."""
        raise NotImplementedError()

    def satellite(self) -> str:
        """Retrieve scene satellite."""
        part = self._fragments[0]

        return part[-2:]

    def tile_id(self) -> str:
        """Retrieve Landsat scene Path row."""
        return self._fragments[2]

    def source(self) -> str:
        """Retrieve Landsat source part from scene id."""
        return self._fragments[0]

    def sensing_date(self) -> datetime:
        """Retrieve Landsat scene sensing date."""
        return datetime.strptime(self._fragments[3], '%Y%m%d')

    def get_band_map(self) -> dict:
        if not self.band_map:
            band_map = {band.common_name: band.name for band in self.collection.bands}
            self.band_map = band_map

        return self.band_map

    def google_path(self) -> Path:
        """Retrieve a formal path for Landsat on Google Provider.

        Example:
            >>> scene = LandsatDigitalNumber08('LC08_L1GT_044034_20130330_20170310_01_T2')
            >>> print(str(scene.google_path()))
            ... 'LC08/01/044/034/LC08_L1GT_044034_20130330_20170310_01_T2'
        """
        first_part = Path(self._fragments[0])

        path = self._fragments[2][:3]
        row = self._fragments[2][-3:]

        path = first_part / '01' / path / row / self.scene_id

        return path

    def path(self, prefix=Config.DATA_DIR):
        """Retrieve relative path on Brazil Data Cube cluster.

        Example:
            >>> scene = LandsatDigitalNumber08('LC08_L1GT_044034_20130330_20170310_01_T2')
            >>> print(str(scene.path()))
            ... '/gfs/Repository/Archive/LC8DN/2013-03/044034'
        """
        year_month = self.sensing_date().strftime('%Y-%m')

        scene_path = Path(prefix or '') / 'Repository/Archive' / self.collection.name / year_month / self.tile_id()

        return scene_path

    def compressed_file(self):
        """Retrieve path to the compressed file (L1)."""
        year_month = self.sensing_date().strftime('%Y-%m')

        product_version = int(self._fragments[0][-2:])

        if product_version == 8:
            collection = 'LC8'
        else:
            collection = '{}{}'.format(self._fragments[0][:2], product_version)

        scene_path = Path(Config.DATA_DIR) / 'Repository/Archive' / collection / year_month / self.tile_id()

        return scene_path / '{}.tar.gz'.format(self.scene_id)

    def compressed_file_bands(self):
        relative_path = self.compressed_file().parent

        files = [
            relative_path / '{}_{}.TIF'.format(self.scene_id, band)
            for band in self.get_band_map().values()
        ]
        files.append(relative_path / '{}_ANG.txt'.format(self.scene_id))
        files.append(relative_path / '{}_MTL.txt'.format(self.scene_id))

        return files

    def get_files(self):
        """Try to find of file names from Brazil Data Cube Cluster.

        Note:
            The scene must be published in order to retrieve the file list.

        Example:
            >>> collection_l1 = Collection.query().filter(Collection.name == 'LC8_DN').first_or_404()
            >>> scene = LandsatProduct('LC08_L1TP_220069_20180618_20180703_01_T1', collection=collection_l1)
            >>> print(str(scene.path()))
            ... ['/gfs/Repository/Archive/LC8_DN/2018-06/220069/LC08_L1TP_220069_20180618_20180703_01_T1_B1.TIF',
            ...  '/gfs/Repository/Archive/LC8_DN/2018-06/220069/LC08_L1TP_220069_20180618_20180703_01_T1_B2.TIF']
        """
        scene_path = self.path()

        scene_id_without_processing_date = '{}_*_{}*'.format(
            '_'.join(self._fragments[:4]),
            '_'.join(self._fragments[-2:])
        )
        logging.debug('Searching on {} with {}'.format(str(scene_path), scene_id_without_processing_date))

        files = scene_path.glob(scene_id_without_processing_date)

        return list([f for f in files if f.suffix.lower() == '.tif'])


def compress_landsat_scene(scene: LandsatProduct, data_dir: str):
    """Compress the Landsat files to tar.gz.

    Args:
        scene - Landsat Product
        data_dir - Path to search for files
    """
    try:
        context_dir = Path(data_dir)

        if not context_dir.exists() or not context_dir.is_dir():
            raise IOError('Invalid directory to compress Landsat. "{}"'.format(data_dir))

        compressed_file_path = Path(data_dir) / scene.compressed_file().name

        files = scene.compressed_file_bands()

        logging.debug('Compressing {}'.format(str(compressed_file_path)))
        # Create compressed file and make available
        with tarfile.open(compressed_file_path, 'w:gz') as compressed_file:
            with working_directory(str(context_dir)):
                for f in files:
                    compressed_file.add(f.name)

    except BaseException:
        logging.error('Could not compress {}.tar.gz'.format(scene.scene_id), exc_info=True)

        raise

    return compressed_file_path
