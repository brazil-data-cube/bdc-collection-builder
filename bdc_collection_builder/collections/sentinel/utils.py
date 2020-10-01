# Python Native
import fnmatch
import logging
import os
import shutil
# 3rdparty
import boto3
import rasterio
from bdc_catalog.models import Collection
from rasterio.enums import Resampling
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

    collection: Collection
    band_map: dict = None

    def __init__(self, scene_id: str, collection: Collection):
        """Build a Sentinel class."""
        self.scene_id = scene_id
        self._fragments = SentinelProduct.parse_scene_id(scene_id)

        if not isinstance(collection, Collection):
            raise RuntimeError(f'The attribute "collection" ({collection}) must be a {Collection}')

        self.collection = collection

        self.get_band_map()

    @property
    def scene_fragments(self):
        """Parse a Sentinel ID and retrieve the parts."""
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
        return self.collection.name

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
        if not self.band_map:
            band_map = {band.name: band.common_name for band in self.collection.bands}
            self.band_map = band_map

        return self.band_map

    def path(self, prefix=Config.DATA_DIR):
        """Retrieve relative path on Brazil Data Cube cluster.

        Example:
            >>> scene = SentinelProduct('S2A_MSIL1C_20150704T101006_N0204_R022_T33UUP_20150704T101337')
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
            scene_path = scene_path / scene_id

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
        """
        scene_path = self.path()

        logging.debug('Searching on {}'.format(str(scene_path)))

        files = scene_path.glob('*')

        return list([f for f in files if f.suffix.lower() == '.tif'])

    def __repr__(self):
        return 'Sentinel(scene={})'.format(self.scene_id)


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
