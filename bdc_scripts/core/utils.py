# Python Native
from json import loads as json_parser
from os import path as resource_path
from zlib import error as zlib_error
from zipfile import BadZipfile, ZipFile

# 3rd-party
import gdal

# BDC Scripts
from bdc_scripts.config import CURRENT_DIR


def get_credentials():
    file = resource_path.join(resource_path.dirname(CURRENT_DIR), 'secrets.json')

    with open(file) as f:
        return json_parser(f.read())


def extractall(file):
    formatted_filename = file.replace('.zip', '.SAFE')

    if not resource_path.exists(formatted_filename):
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

    if resource_path.exists(file_path):
        return file_path

    driver = gdal.GetDriverByName('GTiff')

    data_set = gdal.Open(input_data_set_path, gdal.GA_ReadOnly)

    if data_set is None:
        raise ValueError('Could not open data set "{}"'.format(input_data_set_path))

    dst_ds = driver.CreateCopy(file_path, data_set,  options=['COMPRESS=LZW', 'TILED=YES'])

    gdal.SetConfigOption('COMPRESS_OVERVIEW', 'LZW')
    dst_ds.BuildOverviews('NEAREST', [2, 4, 8, 16, 32])

    del dst_ds
    del data_set

    return file_path
