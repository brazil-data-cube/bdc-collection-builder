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

from bdc_core.decorators.utils import working_directory

from ...config import Config


class LandsatProduct:
    """Define base class for handling Landsat data products."""

    def __init__(self, scene_id: str):
        """Build a Landsat class."""
        self.scene_id = scene_id
        self._fragments = LandsatProduct.parse_scene_id(scene_id)

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
        raise NotImplementedError()

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
        raise NotImplementedError()

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

        scene_path = Path(prefix or '') / 'Repository/Archive' / self.id / year_month / self.tile_id()

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
            >>> scene = LandsatDigitalNumber08('LC08_L1TP_220069_20180618_20180703_01_T1')
            >>> print(str(scene.path()))
            ... ['/gfs/Repository/Archive/LC8DN/2018-06/220069/LC08_L1TP_220069_20180618_20180703_01_T1_B1.TIF',
            ...  '/gfs/Repository/Archive/LC8DN/2018-06/220069/LC08_L1TP_220069_20180618_20180703_01_T1_B2.TIF']
        """
        scene_path = self.path()

        scene_id_without_processing_date = '{}_*_{}*'.format(
            '_'.join(self._fragments[:4]),
            '_'.join(self._fragments[-2:])
        )
        logging.debug('Searching on {} with {}'.format(str(scene_path), scene_id_without_processing_date))

        files = scene_path.glob(scene_id_without_processing_date)

        return list([f for f in files if f.suffix.lower() == '.tif'])


class LandsatDigitalNumber08(LandsatProduct):
    """Landsat 8 Digital Number."""
    id = 'LC8DN'
    level = 1

    def get_band_map(self) -> dict:
        return dict(
            coastal='B1', blue='B2', green='B3', red='B4', nir='B5', swir1='B6', swir2='B7',
            quality='BQA', panchromatic='B8', cirrus='B9', tirs1='B10', tirs2='B11'
        )


class LandsatSurfaceReflectance08(LandsatProduct):
    """Landsat 8 Surface Reflectance."""
    id = 'LC8SR'
    level = 2

    def get_band_map(self) -> dict:
        return dict(
            coastal='sr_band1', blue='sr_band2', green='sr_band3', red='sr_band4', nir='sr_band5',
            swir1='sr_band6', swir2='sr_band7', evi='sr_evi', ndvi='sr_ndvi', quality='Fmask4'
        )


class LandsatNBAR08(LandsatProduct):
    """Landsat 8 Nadir BRDF Adjusted Reflectance."""
    id = 'LC8NBAR'
    level = 3

    def get_band_map(self) -> dict:
        return dict(
            blue='sr_band2', green='sr_band3', red='sr_band4', nir='sr_band5',
            swir1='sr_band6', swir2='sr_band7', quality='pixel_qa'
        )


class LandsatDigitalNumber07(LandsatProduct):
    """Landsat 7 Digital Number."""

    id = 'L7DN'
    level = 1

    def get_band_map(self) -> dict:
        return dict(
            blue='B1', green='B2', red='B3', nir='B4', swir1='B5', tirs='B6',
            swir2='B7', panchromatic='B8', quality='BQA'
        )


class LandsatSurfaceReflectance07(LandsatProduct):
    """Landsat 7 Surface Reflectance."""

    id = 'L7SR'
    level = 2

    def get_band_map(self) -> dict:
        return dict(
            blue='sr_band1', green='sr_band2', red='sr_band3', nir='sr_band4', swir1='sr_band5',
            swir2='sr_band7', evi='sr_evi', ndvi='sr_ndvi', quality='Fmask4'
        )


class LandsatDigitalNumber05(LandsatProduct):
    """Landsat 5 Digital Number."""

    id = 'L5DN'
    level = 1

    def get_band_map(self) -> dict:
        return dict(
            blue='B1', green='B2', red='B3', nir='B4', swir1='B5',
            tirs='B6', swir2='B7', quality='BQA'
        )


class LandsatSurfaceReflectance05(LandsatProduct):
    """Landsat 5 Surface Reflectance."""

    id = 'L5SR'
    level = 2

    def get_band_map(self) -> dict:
        return dict(
            blue='sr_band1', green='sr_band2', red='sr_band3', nir='sr_band4', swir1='sr_band5',
            swir2='sr_band7', evi='sr_evi', ndvi='sr_ndvi', quality='Fmask4'
        )


class LandsatFactory:
    """Define a factory to identify a Landsat product based on scene identifier."""

    map = dict(
        l1=dict(),
        l2=dict(),
        l3=dict()
    )

    def register(self):
        """Initialize factory object."""
        self.map['l1'][LandsatDigitalNumber05.id] = LandsatDigitalNumber05
        self.map['l2'][LandsatSurfaceReflectance05.id] = LandsatSurfaceReflectance05
        self.map['l1'][LandsatDigitalNumber07.id] = LandsatDigitalNumber07
        self.map['l2'][LandsatSurfaceReflectance07.id] = LandsatSurfaceReflectance07
        self.map['l1'][LandsatDigitalNumber08.id] = LandsatDigitalNumber08
        self.map['l2'][LandsatSurfaceReflectance08.id] = LandsatSurfaceReflectance08
        self.map['l3'][LandsatNBAR08.id] = LandsatNBAR08

    def get_from_collection(self, collection: str):
        """Retrieve the respective Landsat driver from given collection."""
        for drivers_by_level in self.map.values():
            for driver_name in drivers_by_level:
                if collection == driver_name:
                    return drivers_by_level[driver_name]

        raise ValueError('Not found a valid driver for {}.'.format(collection))

    def get_from_sceneid(self, scene_id: str, level=1) -> LandsatProduct:
        """Retrieve the respective Landsat driver from given scene id."""
        fragments = LandsatProduct.parse_scene_id(scene_id)

        drivers_by_level = self.map.get('l{}'.format(level)) or dict()

        scene_satellite = int(fragments[0][-2:])

        for key in drivers_by_level:
            satellite = key[1]

            if not satellite.isdigit():
                satellite = key[2]

            satellite = int(satellite)

            if scene_satellite == satellite:
                driver = drivers_by_level[key]

                if driver.level == level:
                    return driver(scene_id)

        raise ValueError('Not found a valid driver for {}'.format(scene_id))


factory = LandsatFactory()


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

        compressed_file_path = scene.compressed_file()

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
