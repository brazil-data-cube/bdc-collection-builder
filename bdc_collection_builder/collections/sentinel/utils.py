# Python Native
import fnmatch
import logging
import os
from os import path as resource_path
# 3rdparty
from rasterio.enums import Resampling
from zipfile import ZipFile
import rasterio
# BDC Scripts
from bdc_collection_builder.config import Config
from bdc_collection_builder.collections.models import RadcorActivity
from datetime import datetime
from pathlib import Path


def get_jp2_files(scene: RadcorActivity):
    """Find all jp2 files in L2A SAFE"""
    sentinel_folder_data = scene.args.get('file', '')
    template = "T*.jp2"
    jp2files = [os.path.join(dirpath, f)
                for dirpath, dirnames, files in os.walk("{0}".format(sentinel_folder_data))
                for f in fnmatch.filter(files, template)]
    if len(jp2files) <= 1:
        template = "L2A_T*.jp2"
        jp2files = [os.path.join(dirpath, f)
                    for dirpath, dirnames, files in os.walk("{0}".format(sentinel_folder_data))
                    for f in fnmatch.filter(files, template)]
        if len(jp2files) <= 1:
            msg = 'No {} files found in {}'.format(template, sentinel_folder_data)
            logging.warning(msg)
            raise FileNotFoundError(msg)

    return jp2files


def get_tif_files(scene: RadcorActivity):
    """Find all tif files in L2A SAFE"""
    sentinel_folder_data = scene.args.get('file', '')
    template = "*.tif"
    tiffiles = [os.path.join(dirpath, f)
                for dirpath, dirnames, files in os.walk("{0}".format(sentinel_folder_data))
                for f in fnmatch.filter(files, template)]
    if len(tiffiles) <= 1:
        template = "L2A_T*.tif"
        tiffiles = [os.path.join(dirpath, f)
                    for dirpath, dirnames, files in os.walk("{0}".format(sentinel_folder_data))
                    for f in fnmatch.filter(files, template)]
        if len(tiffiles) <= 1:
            msg = 'No {} files found in {}'.format(template, sentinel_folder_data)
            logging.warning(msg)
            raise FileNotFoundError(msg)

    return tiffiles


def resample_raster(img_path, upscale_factor = 1/2, out_path = None):
    """resample raster given an upscale factor (1/2 is default)"""
    with rasterio.open(img_path) as dataset:
        # resample data to target shape
        data = dataset.read(
            out_shape=(
                dataset.count,
                int(dataset.width * upscale_factor),
                int(dataset.height * upscale_factor)
            ),
            resampling=Resampling.average
        )
        kwargs = dataset.meta

        # scale image transform
        transform = dataset.transform * dataset.transform.scale(
            (dataset.width / data.shape[-2]),
            (dataset.height / data.shape[-1])
        )

        kwargs['width'] = data.shape[1]
        kwargs['height'] = data.shape[2]
        kwargs['transform'] = transform
        if out_path is not None:
            with rasterio.open(out_path, 'w', **kwargs) as dst:
                dst.write_band(1, data[0])
        return data[0]


def load_img_resampled_to_half(img_path):
    """Load an image resampled using upscale factor 1/2"""
    img = resample_raster(img_path, 1/2).flatten()

    return img


class SentinelProduct:
    """Define base class for handling Sentinel data products."""

    def __init__(self, scene_id: str):
        """Build a Sentinel class."""
        self.scene_id = scene_id
        self._fragments = SentinelProduct.parse_scene_id(scene_id)

    @property
    def scene_fragments(self):
        '''Parse a Sentinel ID and retrieve the parts.'''
        if self._fragments is None:
            self._fragments = SentinelProduct.parse_scene_id(self.scene_id)
        return self._fragments

    @staticmethod
    def parse_scene_id(scene_id: str):
        """Parse a Sentinel Scene Identifier."""
        fragments = scene_id.split('_')

        if len(fragments) != 7:
            raise ValueError('Invalid scene id Sentinel')

        return fragments

    @property
    def id(self) -> str:
        """Retrieve Sentinel Collection ID on Brazil Data Cube."""
        raise NotImplementedError()

    @property
    def level(self) -> int:
        """Retrieve Sentinel Collection Level."""
        raise NotImplementedError()

    def satellite(self) -> str:
        """Retrieve scene satellite."""
        part = self._fragments[0]

        return part[-2:]

    def tile_id(self) -> str:
        """Retrieve Sentinel scene Path row."""
        return self._fragments[5][1:]

    def source(self) -> str:
        """Retrieve Sentinel source part from scene id."""
        return self._fragments[0]

    def sensing_date(self) -> datetime:
        """Retrieve Sentinel scene sensing date."""
        return datetime.strptime(self._fragments[2], '%Y%m%dT%H%M%S')

    def get_band_map(self) -> dict:
        """Retrieve the mapped bands of Sentinel Collection."""
        raise NotImplementedError()

    def path(self, prefix=Config.DATA_DIR):
        """Retrieve relative path on Brazil Data Cube cluster.

        Example:
            >>> scene = Sentinel2TOA('S2A_MSIL1C_20150704T101006_N0204_R022_T33UUP_20150704T101337')
            >>> print(str(scene.path()))
            ... '/gfs/Repository/Archive/S2TOA/2015-07/S2A_MSIL1C_20150704T101006_N0204_R022_T33UUP_20150704T101337.SAFE'
        """
        sensing_date = self.sensing_date()

        year_month = sensing_date.strftime('%Y-%m')

        processing_date = datetime.strptime(self.scene_fragments[6], '%Y%m%dT%H%M%S')

        scene_id = self.scene_id

        scene_path = Path(prefix or '') / 'Repository/Archive' / self.id / year_month

        # TODO: Remove this validation since the sen2cor products generates a new processing date
        # after processor execution.
        if self.id == 'S2SR_SEN28':
            maybe_sceneid = '{}_MSIL2A_{}_N9999_{}_{}_{}'.format(
                self._fragments[0],
                self._fragments[2],
                self._fragments[4],
                self._fragments[5],
                '*'
            )
            # '*T{}.SAFE'.format(processing_date.timetz().strftime('%H%M%S'))
            found = list(scene_path.glob(maybe_sceneid))

            if len(found) == 0:
                logging.warning('No file found on {}'.format(str(scene_path / maybe_sceneid)))
                scene_path = scene_path / '{}.SAFE'.format(self.scene_id)
            else:
                if len(found) > 1:
                    time_processing = processing_date.timetz().strftime('%H%M%S')

                    matched = False

                    for file in found:
                        if file.name.endswith('T{}.SAFE'.format(time_processing)):
                            found = [file]
                            matched = True
                            break

                    if not matched:
                        logging.warning('Potential duplicated scene on {} using {}'.format(str(scene_path),
                                                                                           maybe_sceneid))

                scene_path = found[0]
        else:
            scene_path = scene_path / '{}.SAFE'.format(scene_id)

        return scene_path

    def compressed_file(self):
        """Retrieve path to the compressed file (L1)."""
        year_month = self.sensing_date().strftime('%Y-%m')

        scene_path = Path(Config.DATA_DIR) / 'Repository/Archive/S2_MSI' / year_month

        return scene_path / '{}.zip'.format(self.scene_id)

    def get_files(self):
        """Try to find of file names from Brazil Data Cube Cluster.

        Note:
            The scene must be published in order to retrieve the file list.

        Example:
            >>> scene = Sentinel2SR('S2A_MSIL1C_20150704T101006_N0204_R022_T33UUP_20150704T101337')
            >>> print(str(scene.path()))
            ... ['/gfs/Repository/Archive/S2_MSI_L2_SR_LASRC/2015-07/S2A_MSIL1C_20150704T101006_N0204_R022_T33UUP_20150704T101337.tif',
            ...  '/gfs/Repository/Archive/S2_MSI_L2_SR_LASRC/2015-07/S2A_MSIL1C_20150704T101006_N0204_R022_T33UUP_20150704T101337.tif']
        """
        scene_path = self.path()

        logging.debug('Searching on {}'.format(str(scene_path)))

        files = scene_path.glob('*')

        return list([f for f in files if f.suffix.lower() == '.tif'])

    def __repr__(self):
        return 'Sentinel(scene={})'.format(self.scene_id)


class Sentinel2TOA(SentinelProduct):
    id = 'S2TOA'
    level = 1

    def get_band_map(self):
        return {
            'B01': 'coastal', 'B02': 'blue', 'B03': 'green', 'B04': 'red', 'B05': 'redge1',
            'B06': 'redge2', 'B07': 'redge3', 'B08': 'bnir', 'B8A': 'nir', 'B09': 'wvap',
            'B10': 'cirrus', 'B11': 'swir1', 'B12': 'swir2', 'SCL': 'quality'
        }


class Sentinel2SR(SentinelProduct):
    id = 'S2_MSI_L2_SR_LASRC'
    level = 2

    def get_band_map(self):
        return dict(
            sr_band1='coastal', sr_band2='blue', sr_band3='green', sr_band4='red', sr_band5='redge1',
            sr_band6='redge2', sr_band7='redge3', sr_band8='bnir', sr_band8a='nir', sr_band11='swir1',
            sr_band12='swir2', Fmask4='quality'
        )

    def path(self, prefix=Config.DATA_DIR):
        """Overwrite Sentinel Path generator and remove .SAFE."""
        scene_path = super(Sentinel2SR, self).path(prefix=prefix)

        return scene_path.parent / self.scene_id


class Sentinel2NBAR(Sentinel2SR):
    id = 'S2NBAR'
    level = 3


class SentinelFactory:
    """Define a factory to identify a Sentinel product based on scene identifier."""

    map = dict(
        l1=dict(),
        l2=dict(),
        l3=dict()
    )

    def register(self):
        """Initialize factory object."""
        self.map['l1'][Sentinel2TOA.id] = Sentinel2TOA
        self.map['l2'][Sentinel2SR.id] = Sentinel2SR
        self.map['l3'][Sentinel2NBAR.id] = Sentinel2NBAR

    def get_from_collection(self, collection: str):
        """Retrieve the respective Sentinel driver from given collection."""
        for drivers_by_level in self.map.values():
            for driver_name in drivers_by_level:
                if collection == driver_name:
                    return drivers_by_level[driver_name]

        raise ValueError('Not found a valid driver for {}.'.format(collection))

    def get_from_sceneid(self, scene_id: str, level=1) -> SentinelProduct:
        """Retrieve the respective Sentinel driver from given scene id."""
        fragments = SentinelProduct.parse_scene_id(scene_id)

        drivers_by_level = self.map.get('l{}'.format(level)) or dict()

        product_level = int(fragments[0][1])

        for key in drivers_by_level:
            driver_product_level = key[1]

            driver_product_level = int(driver_product_level)

            if product_level == driver_product_level:
                driver = drivers_by_level[key]

                if driver.level == level:
                    return driver(scene_id)

        raise ValueError('Not found a valid driver for {}'.format(scene_id))


factory = SentinelFactory()
