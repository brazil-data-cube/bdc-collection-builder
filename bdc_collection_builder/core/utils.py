# Python Native
from json import loads as json_parser
from os import remove as resource_remove, path as resource_path
from zlib import error as zlib_error
from zipfile import BadZipfile, ZipFile
import logging

# 3rdparty
from botocore.exceptions import ClientError
from skimage.transform import resize
import boto3
import gdal
import numpy

# BDC Scripts
from bdc_collection_builder.config import CURRENT_DIR, Config


def get_credentials():
    file = resource_path.join(resource_path.dirname(CURRENT_DIR), 'secrets.json')

    with open(file) as f:
        return json_parser(f.read())


def extractall(file):
    archive = ZipFile(file, 'r')
    archive.extractall(resource_path.dirname(file))
    archive.close()


def is_valid(file):
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


def generate_cogs(input_data_set_path, file_path):
    """
    Generate Cloud Optimized GeoTIFF files (COG)

    Args:
        input_data_set_path (str) - Path to the input data set
        file_path (str) - Target data set filename

    Returns:
        Path to COG
    """

    src_ds = gdal.Open(input_data_set_path, gdal.GA_ReadOnly)

    if src_ds is None:
        raise ValueError('Could not open data set "{}"'.format(input_data_set_path))

    driver = gdal.GetDriverByName('MEM')

    src_band = src_ds.GetRasterBand(1)
    data_set = driver.Create('', src_ds.RasterXSize, src_ds.RasterYSize, 1, src_band.DataType)
    data_set.SetGeoTransform( src_ds.GetGeoTransform() )
    data_set.SetProjection( src_ds.GetProjection() )

    data_set_band = data_set.GetRasterBand(1)

    data_set_band.WriteArray( src_band.ReadAsArray() )
    data_set.BuildOverviews("NEAREST", [2, 4, 8, 16, 32, 64])

    driver = gdal.GetDriverByName('GTiff')
    dst_ds = driver.CreateCopy(file_path, data_set, options=["COPY_SRC_OVERVIEWS=YES", "TILED=YES", "COMPRESS=LZW"])

    del src_ds
    del data_set
    del dst_ds

    return file_path


def upload_file(file_name, bucket='bdc-ds-datacube', object_name=None):
    """
    Upload a file to an S3 bucket

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
    """
    Remove file if exists

    Throws Error when user doesn't have access to the file at given path
    """

    if resource_path.exists(file_path):
        resource_remove(file_path)


def generate_evi_ndvi(red_band: str, nir_band: str, blue_bland: str, evi_name: str, ndvi_name: str):
    """
    Generate Normalized Difference Vegetation Index (NDVI) and Enhanced Vegetation Index (EVI)

    Args:
        red_band: Path to the RED band
        nir_band: Path to the NIR band
        blue_bland: Path to the BLUE band
        evi_name: Path to save EVI file
        ndvi_name: Path to save NDVI file

    """
    data_set = gdal.Open(red_band, gdal.GA_ReadOnly)
    raster_xsize = data_set.RasterXSize
    raster_ysize = data_set.RasterYSize
    red = data_set.GetRasterBand(1).ReadAsArray(0, 0, data_set.RasterXSize, data_set.RasterYSize).astype(numpy.float32)/10000.

    # Close data_set
    del data_set

    data_set = gdal.Open(nir_band, gdal.GA_ReadOnly)
    nir = data_set.GetRasterBand(1).ReadAsArray(0, 0, data_set.RasterXSize, data_set.RasterYSize).astype(numpy.float32)/10000.
    nir = resize(nir, red.shape, order=1, preserve_range=True).astype(numpy.float32)

    del data_set
    data_set = gdal.Open(blue_bland, gdal.GA_ReadOnly)
    blue = data_set.GetRasterBand(1).ReadAsArray(0, 0, data_set.RasterXSize, data_set.RasterYSize).astype(numpy.float32)/10000.

    # Create the ndvi image data_set
    remove_file(ndvi_name)

    driver = gdal.GetDriverByName('GTiff')

    raster_ndvi = (10000 * (nir - red) / (nir + red + 0.0001)).astype(numpy.int16)
    ndvi_data_set = driver.Create(ndvi_name, raster_xsize, raster_ysize, 1, gdal.GDT_Int16, options=['COMPRESS=LZW',
                                                                                                        'TILED=YES'])
    ndvi_data_set.SetGeoTransform(data_set.GetGeoTransform())
    ndvi_data_set.SetProjection(data_set.GetProjection())
    ndvi_data_set.GetRasterBand(1).WriteArray(raster_ndvi)
    del ndvi_data_set

    # Create the evi image data set
    remove_file(evi_name)
    evi_data_set = driver.Create(evi_name, raster_xsize, raster_ysize, 1, gdal.GDT_Int16, options=['COMPRESS=LZW',
                                                                                                    'TILED=YES'])
    raster_evi = (10000 * 2.5 * (nir - red)/(nir + 6. * red - 7.5 * blue + 1)).astype(numpy.int16)
    evi_data_set.SetGeoTransform(data_set.GetGeoTransform())
    evi_data_set.SetProjection(data_set.GetProjection())
    evi_data_set.GetRasterBand(1).WriteArray(raster_evi)
    del raster_evi
    del evi_data_set

    del data_set
