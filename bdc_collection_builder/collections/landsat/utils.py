#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Define the Collection Builder utilities for Landsat data products."""

import logging
from datetime import datetime
from pathlib import Path

from ...config import Config


class LandsatProduct:
    """Define base class for handling Landsat data products."""

    def __init__(self, scene_id: str):
        """Build a Landsat class."""
        self.scene_id = scene_id
        self._fragments = LandsatProduct.parse_scene_id(scene_id)

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

    def tile_id(self) -> str:
        """Retrieve Landsat scene Path row."""
        return self._fragments[2]

    def sensing_date(self) -> datetime:
        """Retrieve Landsat scene sensing date."""
        return datetime.strptime(self._fragments[3], '%Y%m%d')

    def get_band_names(self):
        raise NotImplementedError()

    def google_path(self) -> Path:
        """Retrieve a formal path for Landsat on Google Provider.

        Example:
            >>> scene = Landsat8DN('LC08_L1GT_044034_20130330_20170310_01_T2')
            >>> print(str(scene.google_path()))
            ... LC08/01/044/034/LC08_L1GT_044034_20130330_20170310_01_T2
        """
        first_part = Path(self._fragments[0])

        path = self._fragments[2][:3]
        row = self._fragments[2][-3:]

        path = first_part / '01' / path / row / self.scene_id

        return path

    def path(self):
        """Retrieve relative path on Brazil Data Cube cluster.

        Example:
            >>> scene = Landsat8DN('LC08_L1GT_044034_20130330_20170310_01_T2')
            >>> print(str(scene.path()))
            ... Repository/Archive/LC8DN/2013-03/044034
        """
        year_month = self.sensing_date().strftime('%Y-%m')

        scene_path = Path('Repository/Archive') / self.id / year_month / self.tile_id()

        return scene_path

    def get_files(self, path_prefix=Config.DATA_DIR):
        """Try to find of file names from Brazil Data Cube Cluster.

        Note:
            The scene must be published in order to retrieve the file list.

        Example:
            >>> scene = Landsat8DN('LC08_L1TP_220069_20180618_20180703_01_T1/')
            >>> print(str(scene.path()))
            ... [/gfs/Repository/Archive/LC8DN/2018-06/220069/LC08_L1TP_220069_20180618_20180703_01_T1_B1.TIF,
            ...  /gfs/Repository/Archive/LC8DN/2018-06/220069/LC08_L1TP_220069_20180618_20180703_01_T1_B2.TIF]
        """
        scene_path = Path(path_prefix) / self.path()

        scene_id_without_processing_date = '{}_*_{}*'.format(
            '_'.join(self._fragments[:4]),
            '_'.join(self._fragments[-2:])
        )
        logging.debug('Searching on {} with {}'.format(str(scene_path), scene_id_without_processing_date))

        files = scene_path.glob(scene_id_without_processing_date)

        return list([f for f in files if f.suffix.lower() == '.tif'])


class Landsat8DN(LandsatProduct):
    """Landsat 8 Digital Number."""
    id = 'LC8DN'
    level = 1


class Landsat8SR(LandsatProduct):
    """Landsat 8 Surface Reflectance."""
    id = 'LC8SR'
    level = 2


class Landsat7DN(LandsatProduct):
    """Landsat 7 Digital Number."""

    id = 'L7DN'
    level = 1


class Landsat7SR(LandsatProduct):
    """Landsat 7 Surface Reflectance."""

    id = 'L7SR'
    level = 2


class Landsat5DN(LandsatProduct):
    """Landsat 5 Digital Number."""

    id = 'L5DN'
    level = 1


class Landsat5SR(LandsatProduct):
    """Landsat 5 Surface Reflectance."""

    id = 'L5SR'
    level = 2


class LandsatFactory:
    """Define a factory to identify a Landsat product based on scene identifier."""

    _map = dict(
        l1=dict(),
        l2=dict(),
    )

    def __init__(self):
        pass

    def register(self):
        """Initialize factory object."""
        self._map['l1'][Landsat5DN.id] = Landsat5DN
        self._map['l2'][Landsat5SR.id] = Landsat5SR
        self._map['l1'][Landsat7DN.id] = Landsat7DN
        self._map['l2'][Landsat7SR.id] = Landsat7SR
        self._map['l1'][Landsat8DN.id] = Landsat8DN
        self._map['l2'][Landsat8SR.id] = Landsat8SR

    def get_from_collection(self, collection: str):
        """Retrieve the respective Landsat driver from given collection."""
        for drivers_by_level in self._map.values():
            for driver_name in drivers_by_level:
                if collection == driver_name:
                    return drivers_by_level[driver_name]

        raise ValueError('Not found a valid driver for {}.'.format(collection))

    def get_from_sceneid(self, scene_id: str, level=1) -> LandsatProduct:
        """Retrieve the respective Landsat driver from given scene id."""
        fragments = LandsatProduct.parse_scene_id(scene_id)

        drivers_by_level = self._map.get('l{}'.format(level)) or dict()

        scene_satellite = int(fragments[0][-2:])

        for key in drivers_by_level:
            length = len(key)

            satellite: str = key[1]

            if not satellite.isdigit():
                satellite = key[2]

            satellite = int(satellite)

            if scene_satellite == satellite:
                driver = drivers_by_level[key]

                if driver.level == level:
                    return driver(scene_id)

        raise ValueError('Not found a valid driver for {}'.format(scene_id))


factory = LandsatFactory()
