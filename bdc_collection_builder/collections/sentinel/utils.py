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
from bdc_collection_builder.collections.models import RadcorActivity


def get_jp2_files(scene: RadcorActivity):
    """Get all jp2 files in L2A SAFE"""
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
    """Get all tif files in L2A SAFE"""
    sentinel_folder_data = scene.args.get('file', '')
    template = "T*.tif"
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
