# Python Native
import glob
import logging
import os
import re

from pathlib import Path
from shutil import copy

# BDC Scripts
from bdc_db.models import CollectionItem
from bdc_collection_builder.config import Config
from bdc_collection_builder.collections.nbar import process_NBAR
from bdc_collection_builder.collections.utils import load_img
from bdc_collection_builder.collections.models import RadcorActivity


def load_landsat_angles(productdir):
    """Load Landsat Angle bands."""
    img_list = [f for f in glob.glob(productdir + "/*.tif", recursive=True)]
    logging.debug('Load Landsat Angles')
    pattern = re.compile('.*_solar_zenith_.*')
    sz_path = list(filter(pattern.match, img_list))[0]
    pattern = re.compile('.*_solar_azimuth_.*')
    sa_path = list(filter(pattern.match, img_list))[0]
    pattern = re.compile('.*_sensor_zenith_.*')
    vz_path = list(filter(pattern.match, img_list))[0]
    pattern = re.compile('.*_sensor_azimuth_.*')
    va_path = list(filter(pattern.match, img_list))[0]

    return sz_path, sa_path, vz_path, va_path


def landsat_NBAR(sz_path, sa_path, vz_path, va_path, productdir, target_dir):
    """Prepare landsat angle bands and process NBAR."""
    ### Landsat-8 data set ###
    satsen = 'LC8'
    bands10m = ['sr_band2','sr_band3','sr_band4', 'sr_band5','sr_band6','sr_band7']
    pars_array_index = {'sr_band2': 0, 'sr_band3': 1, 'sr_band4': 2, 'sr_band5': 3, 'sr_band6': 4, 'sr_band7': 5}

    band_sz = load_img(sz_path)
    band_sa = load_img(sa_path)
    band_vz = load_img(vz_path)
    band_va = load_img(va_path)
    logging.debug('Harmonization ...')
    process_NBAR(productdir, bands10m, band_sz, band_sa, band_vz, band_va, satsen, pars_array_index, target_dir)

    return


def landsat_harmonize(collection_item: CollectionItem, scene: RadcorActivity):
    """Landsat harmonization."""
    identifier = scene.sceneid
    cc = identifier.split('_')
    pathrow = cc[2]
    date = cc[3]
    yyyymm = '{}-{}'.format(date[0:4], date[4:6])

    productdir = scene.args.get('file')

    target_dir = str(Path(Config.DATA_DIR) / 'Repository/Archive/{}/{}/{}'.format(collection_item.collection_id, yyyymm, pathrow))
    os.makedirs(target_dir, exist_ok=True)

    logging.debug('Loading Angles from {} ...'.format(productdir))
    sz_path, sa_path, vz_path, va_path = load_landsat_angles(productdir)
    landsat_NBAR(sz_path, sa_path, vz_path, va_path, productdir, target_dir)

    #COPY quality band
    pattern = re.compile('.*pixel_qa.*')
    img_list = [f for f in glob.glob(productdir + "/*.tif", recursive=True)]
    qa_path = list(filter(pattern.match, img_list))[0]
    copy(qa_path, target_dir)

    return target_dir
