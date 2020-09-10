# Python Native
import fnmatch
import logging
import os
import shutil
from tempfile import TemporaryDirectory
# 3rdparty
from typing import Optional

import boto3
import numpy
import rasterio
from bdc_catalog.models import Band, Collection
from rasterio.enums import Resampling
# BDC Scripts
from bdc_collection_builder.collections.utils import generate_cogs
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


def post_processing(quality_file_path: str, collection: Collection, scenes: dict, resample_to=None):
    """Stack the merge bands in order to apply a filter on the quality band.

    We have faced some issues regarding `nodata` value in spectral bands, which was resulting
    in wrong provenance date on STACK data cubes, since the Fmask tells the pixel is valid (0) but a nodata
    value is found in other bands.
    To avoid that, we read all the others bands, seeking for `nodata` value. When found, we set this to
    nodata in Fmask output::

        Quality             Nir                   Quality

        0 0 2 4      702  876 7000 9000      =>    0 0 2 4
        0 0 0 0      687  987 1022 1029      =>    0 0 0 0
        0 2 2 4    -9999 7100 7322 9564      =>  255 2 2 4

    Notes:
        It may take too long to execute for a large grid.

    Args:
         quality_file_path: Path to the cloud masking file.
         collection: The collection instance.
         scenes: Map of band and file path
         resample_to: Resolution to re-sample. Default is None, which uses default value.
    """
    quality_file_path = Path(quality_file_path)
    band_names = [band_name for band_name in scenes.keys() if band_name.lower() not in ('ndvi', 'evi', 'fmask4')]

    bands = Band.query().filter(
        Band.collection_id == collection.id,
        Band.name.in_(band_names)
    ).all()

    options = dict()

    with TemporaryDirectory() as tmp:
        temp_file = Path(tmp) / quality_file_path.name

        # Copy to temp dir
        shutil.copyfile(quality_file_path, temp_file)

        if resample_to:
            with rasterio.open(str(quality_file_path)) as ds:
                ds_transform = ds.profile['transform']

                options.update(ds.meta.copy())

                factor = ds_transform[0] / resample_to

                options['width'] = ds.profile['width'] * factor
                options['height'] = ds.profile['height'] * factor

                transform = ds.transform * ds.transform.scale((ds.width / options['width']), (ds.height / options['height']))

                options['transform'] = transform

                nodata = options.get('nodata') or 255
                options['nodata'] = nodata

                raster = ds.read(
                    out_shape=(
                        ds.count,
                        int(options['height']),
                        int(options['width'])
                    ),
                    resampling=Resampling.nearest
                )

                with rasterio.open(str(temp_file), mode='w', **options) as temp_ds:
                    temp_ds.write_band(1, raster[0])

                # Build COG
                generate_cogs(str(temp_file), str(temp_file))

        with rasterio.open(str(temp_file), **options) as quality_ds:
            blocks = list(quality_ds.block_windows())
            profile = quality_ds.profile
            nodata = profile.get('nodata', 255)
            raster_merge = quality_ds.read(1)
            for _, block in blocks:
                nodata_positions = []

                row_offset = block.row_off + block.height
                col_offset = block.col_off + block.width

                for band in bands:
                    band_file = scenes[band.name]['file']

                    with rasterio.open(str(band_file)) as ds:
                        raster = ds.read(1, window=block)

                    nodata_found = numpy.where(raster == -9999)
                    raster_nodata_pos = numpy.ravel_multi_index(nodata_found, raster.shape)
                    nodata_positions = numpy.union1d(nodata_positions, raster_nodata_pos)

                if len(nodata_positions):
                    raster_merge[block.row_off: row_offset, block.col_off: col_offset][
                        numpy.unravel_index(nodata_positions.astype(numpy.int64), raster.shape)] = nodata

        save_as_cog(str(temp_file), raster_merge, **profile)

        # Move right place
        shutil.move(str(temp_file), str(quality_file_path))


def save_as_cog(destination: str, raster, mode='w', **profile):
    """Save the raster file as Cloud Optimized GeoTIFF.

    See Also:
        Cloud Optimized GeoTiff https://gdal.org/drivers/raster/cog.html

    Args:
        destination: Path to store the data set.
        raster: Numpy raster values to persist in disk
        mode: Default rasterio mode. Default is 'w' but you also can set 'r+'.
        **profile: Rasterio profile values to add in dataset.
    """
    with rasterio.open(str(destination), mode, **profile) as dataset:
        if profile.get('nodata'):
            dataset.nodata = profile['nodata']

        dataset.write_band(1, raster)
        dataset.build_overviews([2, 4, 8, 16, 32, 64], Resampling.nearest)
        dataset.update_tags(ns='rio_overview', resampling='nearest')

    generate_cogs(str(destination), str(destination))


def _s3_bucket_instance(bucket: str):
    s3 = boto3.resource(
        's3', region_name=Config.AWS_REGION_NAME,
        aws_access_key_id=Config.AWS_ACCESS_KEY_ID, aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY
    )

    bucket = s3.Bucket(bucket)

    return s3, bucket


class DataSynchronizer:
    def __init__(self, file_path: str, bucket: str = Config.COLLECTION_BUILDER_SYNC_BUCKET):
        self.file_path = Path(file_path)
        self.bucket = bucket
        self.prefix = Path(Config.DATA_DIR) / 'Repository/Archive'

    def __instance(self):
        s3 = boto3.resource(
            's3', region_name=Config.AWS_REGION_NAME,
            aws_access_key_id=Config.AWS_ACCESS_KEY_ID, aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY
        )

        bucket = s3.Bucket(self.bucket)

        return s3, bucket

    def check_data(self):
        """Try to check file availability both in local and AWS.

        Notes:
            To activate this feature, make sure to set `COLLECTION_BUILDER_SYNC=True`.

        This method aim's to solve the problem to work with multiple workers on AWS.
        Since ``Collection-Builder`` deal with multiprocessing tasks, it requires an shared volume where
        the workers can manipulate the data and then pass the result to the other node to continue process stream.

        On Amazon Web Service environment, this feature is only available in North American Servers, however, the
        Sentinel-2 data server is located in Frankfurt.
        To do that, we have created this feature to the workers store the result temporally data in
        the Amazon Simple Storage Service (S3).

        Warning:
            Currently, ``Collection-Builder`` is not fully supporting auto-removal data from AWS on Exceptions.

        Args:
            file_path - Path to file / folder to require from AWS.
            bucket - Bucket to check for. Default is ``Config.COLLECTION_BUILDER_SYNC_BUCKET``
        """
        expected_file_path = Path(self.file_path)

        # When required file not in disk, seek in the bucket
        if not expected_file_path.exists():
            logging.info(f'File {str(self.file_path)} is not available here. Checking in bucket {self.bucket}')

            _, bucket = _s3_bucket_instance(self.bucket)

            relative_path = expected_file_path.relative_to(self.prefix)

            for blob in bucket.objects.filter(Prefix=str(relative_path)):
                destination = self.prefix / blob.key

                destination.parent.mkdir(exist_ok=True, parents=True)

                bucket.download_file(blob.key, str(destination))

    @staticmethod
    def is_remote_sync_configured():
        return Config.COLLECTION_BUILDER_SYNC

    def remove_data(self, raise_error=False):
        path = Path(self.file_path)

        if path.exists():
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            elif path.is_file():
                path.unlink()

        _, bucket = _s3_bucket_instance(self.bucket)

        relative_path = path.relative_to(self.prefix)

        try:
            bucket.delete_objects(
                Delete=dict(
                    Objects=[
                        dict(Key=str(relative_path))
                    ]
                )
            )
            logging.info(f'Entry {str(relative_path)} removed from {self.bucket}')
        except Exception as e:
            logging.error(f'Cannot remove {str(relative_path)} - {str(e)}')
            if raise_error:
                raise e

    def sync_data(self, file_path: str = None, bucket: str = None, auto_remove=False):
        expected_file_path = Path(file_path or self.file_path)

        if not expected_file_path.exists():
            raise RuntimeError(f'File {str(expected_file_path)} does not exists.')

        _bucket = bucket or self.bucket
        logging.info(f'Uploading {str(self.file_path)} to bucket {_bucket}')

        _, bucket = _s3_bucket_instance(_bucket)

        relative_path = expected_file_path.relative_to(self.prefix)

        if expected_file_path.is_file():
            bucket.upload_file(str(expected_file_path), str(relative_path))

            if auto_remove:
                expected_file_path.unlink()
        else:
            for path in expected_file_path.iterdir():
                if path.is_dir():
                    continue

                item_relative = path.relative_to(self.prefix)

                bucket.upload_file(str(path), str(item_relative))

                if auto_remove:
                    path.unlink()


factory = SentinelFactory()
