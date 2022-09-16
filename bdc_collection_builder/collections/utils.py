#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Define Brazil Data Cube utils."""


# Python Native
import contextlib
import datetime
import logging
import shutil
import tarfile
import warnings
from json import loads as json_parser
from os import path as resource_path
from os import remove as resource_remove
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Tuple
from urllib3.exceptions import InsecureRequestWarning
from zipfile import BadZipfile, ZipFile
from zlib import error as zlib_error

# 3rdparty
import boto3
import numpy
import rasterio
import rasterio.features
import rasterio.warp
import requests

import shapely
import shapely.geometry
from bdc_catalog.models import Band, Collection, GridRefSys, MimeType, ResolutionUnit, db
from bdc_catalog.utils import multihash_checksum_sha256
from bdc_collectors.base import BaseProvider
from bdc_collectors.ext import CollectorExtension
from botocore.exceptions import ClientError
from flask import current_app
from rasterio.warp import Resampling
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
# Builder
from werkzeug.exceptions import abort

from ..config import CURRENT_DIR, Config
from .models import ProviderSetting


def get_or_create_model(model_class, defaults=None, engine=None, **restrictions):
    """Get or create Brazil Data Cube model.

    Utility method for looking up an object with the given restrictions, creating one if necessary.

    Args:
        model_class (BaseModel) - Base Model of Brazil Data Cube DB
        defaults (dict) - Values to fill out model instance
        restrictions (dict) - Query Restrictions
    Returns:
        BaseModel Retrieves model instance
    """
    if not engine:
        engine = db

    instance = engine.session.query(model_class).filter_by(**restrictions).first()

    if instance:
        return instance, False

    params = dict((k, v) for k, v in restrictions.items())

    params.update(defaults or {})
    instance = model_class(**params)

    engine.session.add(instance)

    return instance, True


def load_img(img_path):
    """Load an image."""
    try:
        with rasterio.open(img_path) as dataset:
            img = dataset.read(1).flatten()
        return img
    except:
        logging.error('Cannot find {}'.format(img_path))
        raise RuntimeError('Cannot find {}'.format(img_path))


def extractall(file, destination=None):
    """Extract zipfile."""
    archive = ZipFile(file, 'r')

    if destination is None:
        destination = resource_path.dirname(file)

    archive.extractall(destination)
    archive.close()


def get_credentials():
    """Retrieve global secrets with credentials."""
    file = resource_path.join(resource_path.dirname(CURRENT_DIR), 'secrets.json')

    with open(file) as f:
        return json_parser(f.read())


def generate_cogs(input_data_set_path, file_path, profile='deflate', profile_options=None, **options):
    """Generate Cloud Optimized GeoTIFF files (COG).

    Example:
        >>> tif_file = '/path/to/tif'
        >>> generate_cogs(tif_file, '/tmp/cog.tif')

    Args:
        input_data_set_path (str) - Path to the input data set
        file_path (str) - Target data set filename
        profile (str) - A COG profile based in `rio_cogeo.profiles`.
        profile_options (dict) - Custom options to the profile.

    Returns:
        Path to COG.
    """
    if profile_options is None:
        profile_options = dict()

    output_profile = cog_profiles.get(profile)
    output_profile.update(dict(BIGTIFF="IF_SAFER"))
    output_profile.update(profile_options)

    # Add option to generate Cloud Optimized GeoTIFF file in memory instead inline temp file.
    options.setdefault('in_memory', True)

    # Dataset Open option (see gdalwarp `-oo` option)
    config = dict(
        GDAL_NUM_THREADS="ALL_CPUS",
        GDAL_TIFF_INTERNAL_MASK=True,
        GDAL_TIFF_OVR_BLOCKSIZE="128",
    )

    cog_translate(
        str(input_data_set_path),
        str(file_path),
        output_profile,
        config=config,
        quiet=True,
        **options,
    )
    return str(file_path)


def is_valid_compressed(file):
    """Check tar gz or zip is valid."""
    try:
        archive = ZipFile(file, 'r')
        try:
            corrupt = archive.testzip()
        except zlib_error:
            corrupt = True
        archive.close()
    except BadZipfile:
        corrupt = True

    return not corrupt


def extract_and_get_internal_name(zip_file_name, extract_to=None):
    """Extract zipfile and return internal folder path."""
    # Check if file is valid
    valid = is_valid_compressed(zip_file_name)

    if not valid:
        raise IOError('Invalid zip file "{}"'.format(zip_file_name))
    else:
        extractall(zip_file_name, destination=extract_to)

        # Get extracted zip folder name
        with ZipFile(zip_file_name) as zipObj:
            listOfiles = zipObj.namelist()
            extracted_file_path = listOfiles[0].split('/')[0] if listOfiles[0].endswith('/') else listOfiles[0]

        return extracted_file_path


def upload_file(file_name, bucket='bdc-ds-datacube', object_name=None):
    """Upload a file to an S3 bucket.

    Adapted code from boto3 example.

    Args:
        file_name (str|_io.TextIO): File to upload
        bucket (str): Bucket to upload to
        object_name (str): S3 object name. If not specified then file_name is used
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3', region_name=Config.AWS_REGION_NAME, aws_access_key_id=Config.AWS_ACCESS_KEY_ID, aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY)
    try:
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def remove_file(file_path: str):
    """Remove file if exists.

    Throws Error when user doesn't have access to the file at given path
    """
    if resource_path.exists(file_path):
        resource_remove(file_path)


def create_asset_definition(href: str, mime_type: str, role: List[str], absolute_path: str,
                            created=None, is_raster=False):
    """Create a valid asset definition for collections.

    TODO: Generate the asset for `Item` field with all bands

    Args:
        href - Relative path to the asset
        mime_type - Asset Mime type str
        role - Asset role. Available values are: ['data'], ['thumbnail']
        absolute_path - Absolute path to the asset. Required to generate check_sum
        created - Date time str of asset. When not set, use current timestamp.
        is_raster - Flag to identify raster. When set, `raster_size` and `chunk_size` will be set to the asset.
    """
    fmt = '%Y-%m-%dT%H:%M:%S'
    _now_str = datetime.datetime.utcnow().strftime(fmt)

    if created is None:
        created = _now_str
    elif isinstance(created, datetime.datetime):
        created = created.strftime(fmt)

    asset = {
        'href': str(href),
        'type': mime_type,
        'bdc:size': Path(absolute_path).stat().st_size,
        'checksum:multihash': multihash_checksum_sha256(str(absolute_path)),
        'roles': role,
        'created': created,
        'updated': _now_str
    }

    if is_raster:
        with rasterio.open(str(absolute_path)) as data_set:
            asset['bdc:raster_size'] = dict(
                x=data_set.shape[1],
                y=data_set.shape[0],
            )

            chunk_x, chunk_y = data_set.profile.get('blockxsize'), data_set.profile.get('blockxsize')

            if chunk_x is None or chunk_x is None:
                return asset

            asset['bdc:chunk_size'] = dict(x=chunk_x, y=chunk_y)

    return asset


def raster_extent(file_path: str, epsg='EPSG:4326') -> shapely.geometry.Polygon:
    """Get raster extent in arbitrary CRS.

    Args:
        file_path (str): Path to image
        epsg (str): EPSG Code of result crs

    Returns:
        dict: geojson-like geometry
    """
    with rasterio.open(str(file_path)) as data_set:
        _geom = shapely.geometry.mapping(shapely.geometry.box(*data_set.bounds))
        return shapely.geometry.shape(rasterio.warp.transform_geom(data_set.crs, epsg, _geom, precision=6))


def raster_convexhull(file_path: str, epsg='EPSG:4326', no_data=None) -> dict:
    """Get raster image footprint.

    Args:
        file_path (str): image file
        epsg (str): geometry EPSG
        no_data: Use custom no data value. Default is dataset.nodata

    See:
        https://rasterio.readthedocs.io/en/latest/topics/masks.html
    """
    with rasterio.open(str(file_path)) as data_set:
        # Read raster data, masking nodata values
        data = data_set.read(1, masked=True)

        if no_data is not None:
            data[data == no_data] = numpy.ma.masked

        data[data != numpy.ma.masked] = 1
        data[data == numpy.ma.masked] = 0

        data = data.astype(numpy.uint8)
        # Create mask, which 1 represents valid data and 0 nodata
        geoms = []
        for geom, _ in rasterio.features.shapes(data, mask=data, transform=data_set.transform):
            geom = rasterio.warp.transform_geom(data_set.crs, epsg, geom, precision=6)

            geoms.append(shapely.geometry.shape(geom))

        if len(geoms) == 1:
            return geoms[0].convex_hull

        multi_polygons = shapely.geometry.MultiPolygon(geoms)

        return multi_polygons.convex_hull


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
            nodata = profile.get('nodata') or 255
            raster_merge = quality_ds.read(1)
            for _, block in blocks:
                nodata_positions = []

                row_offset = block.row_off + block.height
                col_offset = block.col_off + block.width

                for band in bands:
                    band_file = scenes[band.name]

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


def is_valid_compressed_file(file_path: str) -> bool:
    """Check if given file is a compressed file and hen check file integrity."""
    if file_path.endswith('.zip'):
        return is_valid_compressed(file_path)
    if file_path.endswith('.tar'):
        return is_valid_tar(file_path)
    if file_path.endswith('.tar.gz'):
        return is_valid_tar_gz(file_path)
    if file_path.endswith('.hdf'):
        from .hdf import is_valid
        return is_valid(file_path)
    return True


def is_valid_tar(file_path: str) -> bool:
    """Check file integrity of a tar file."""
    try:
        with tarfile.open(file_path) as tfile:
            _ = tfile.getmembers()

            return True
    except tarfile.TarError:
        return False


def is_valid_tar_gz(file_path: str):
    """Check tar file integrity."""
    import subprocess
    try:
        retcode = subprocess.call(['gunzip', '-t', file_path])
        return retcode == 0
    except BaseException:
        return False


def get_provider(catalog, **kwargs) -> Tuple[ProviderSetting, BaseProvider]:
    """Retrieve the bdc_catalog.models.Provider instance with the respective Data Provider."""
    provider: ProviderSetting = (
        ProviderSetting.query()
        .filter(ProviderSetting.driver_name == catalog)
        .first_or_404(f'Provider "{catalog}" not found.')
    )

    provider_type = get_provider_type(catalog)

    if provider_type is None:
        abort(400, f'Catalog {catalog} not supported.')

    options = dict(**kwargs)
    options.setdefault('lazy', True)
    options.setdefault('progress', False)

    if isinstance(provider.credentials, dict):
        opts = dict(**provider.credentials)
        opts.update(options)
        provider_ext = provider_type(**opts)
    else:
        provider_ext = provider_type(*provider.credentials, **options)

    return provider, provider_ext


def get_provider_type(catalog: str):
    """Retrieve the driver for Data Collector.

    Seek in bdc-collectors app for the driver type for catalog representation.
    """
    ext = get_collector_ext()
    return ext.get_provider(catalog)


def get_collector_ext() -> CollectorExtension:
    """Retrieve the loaded collector extension (BDC-Collectors)."""
    return current_app.extensions['bdc_collector']


def get_epsg_srid(file_path: str) -> int:
    """Get the Authority Code from a data set path.

    Note:
        This function depends GDAL.

    When no code found, returns None.
    """
    with rasterio.open(str(file_path)) as ds:
        crs = ds.crs

    if crs is not None and crs.to_epsg():
        return crs.to_epsg()

    from osgeo import gdal, osr

    ref = osr.SpatialReference()

    if crs is None:
        ds = gdal.Open(str(file_path))
        wkt = ds.GetProjection()
    else:
        wkt = crs.to_wkt()

    ref.ImportFromWkt(wkt)

    code = ref.GetAuthorityCode(None)
    return int(code) if str(code).isnumeric() else None


def is_sen2cor(collection: Collection) -> bool:
    """Check if the given collection is a Sen2cor product."""
    if collection._metadata and collection._metadata.get('processors'):
        processors = collection._metadata['processors']

        for processor in processors:
            if processor.get('name', '').lower() == 'sen2cor':
                return True

    return False


_settings = requests.Session.merge_environment_settings


@contextlib.contextmanager
def safe_request():
    """Define a decorator to disable any SSL Certificate Validation while requesting data.

    This snippet was adapted from https://stackoverflow.com/questions/15445981/how-do-i-disable-the-security-certificate-check-in-python-requests.
    """
    opened_adapters = set()

    if not Config.DISABLE_SSL:
        yield

    logging.info('Disabling SSL validation')

    def _merge_environment_settings(self, url, proxies, stream, verify, cert):
        """Stack the opened contexts into heap and set all the active adapters with verify=False."""
        opened_adapters.add(self.get_adapter(url))

        settings = _settings(self, url, proxies, stream, verify, cert)
        settings['verify'] = False

        return settings

    requests.Session.merge_environment_settings = _merge_environment_settings

    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', InsecureRequestWarning)
            yield
    finally:
        requests.Session.merge_environment_settings = _settings

        for adapter in opened_adapters:
            try:
                adapter.close()
            except:
                pass


def create_collection(name: str, version: int, bands: list, category: str = 'eo', **kwargs) -> Tuple[Collection, bool]:
    collection = (
        Collection.query()
        .filter(Collection.name == name,
                Collection.version == version)
        .first()
    )
    if collection is not None:
        return collection, False

    with db.session.begin_nested():
        collection = Collection(name=name, version=version)
        collection.collection_type = kwargs.get('collection_type', 'collection')
        collection.grs = GridRefSys.query().filter(GridRefSys.name == kwargs.get('grid_ref_sys')).first()
        collection.description = kwargs.get('description')
        collection.title = kwargs.get('title', collection.name)
        collection.category = category
        collection.is_available = kwargs.get('is_available', True)

        for band in bands:
            band_obj = Band(collection=collection, name=band['name'])
            for prop, value in band.items():
                if prop == 'mime_type':
                    band_obj.mime_type = MimeType.query().filter(MimeType.name == value).first()
                elif prop == 'resolution_unit':
                    band_obj.resolution_unit = ResolutionUnit.query().filter(ResolutionUnit.name == value).first()
                else:
                    setattr(band_obj, prop, value)
            db.session.add(band_obj)

        db.session.add(collection)
    db.session.commit()

    return collection, True
