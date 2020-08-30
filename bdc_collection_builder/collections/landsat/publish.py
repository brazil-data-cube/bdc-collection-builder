#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Describe Landsat 8 publish generation."""

# Python Native
import logging
from os import makedirs, path as resource_path
from pathlib import Path
from shutil import unpack_archive

# 3rdparty
from bdc_catalog.models import Band, Collection, Item, Quicklook, db
from gdal import GA_ReadOnly, GetDriverByName, Open as GDALOpen

# Builder
from ...config import Config
from ...constants import COG_MIME_TYPE
from ...db import add_instance, commit, db_aws
from ..forms import CollectionItemForm
from ..models import RadcorActivity
from ..utils import create_quick_look, generate_evi_ndvi, generate_cogs, is_valid_tif, create_asset_definition
from .utils import factory


DEFAULT_QUICK_LOOK_BANDS = ["swir2", "nir", "red"]


def generate_vi(productdir, files):
    """Prepare and generate Vegetation Index of Landsat Products."""
    fragments = Path(str(files['red'])).stem.split('_')
    pattern = "_".join(fragments[:-1])

    ndvi_name = resource_path.join(productdir, pattern + '_ndvi.tif')
    evi_name = resource_path.join(productdir, pattern + '_evi.tif')
    files['ndvi'] = ndvi_name
    files['evi'] = evi_name

    generate_evi_ndvi(str(files['red']), str(files['nir']), str(files['blue']), evi_name, ndvi_name)

    if not is_valid_tif(ndvi_name) or not is_valid_tif(evi_name):
        raise RuntimeError('Not Valid Vegetation index file')


def uncompress(file_path, destination):
    """Uncompress Landsat 8 DN."""
    unpack_archive(file_path, destination)
    return destination


def apply_valid_range(input_data_set_path: str, file_path: str) -> str:
    """Apply Valid Range -10000 -> 10000.

    Args:
        input_data_set_path (str) - Path to the input data set
        file_path (str) - Target data set filename
    Returns:
        Path to valid_range_image
    """
    src_ds = GDALOpen(input_data_set_path, GA_ReadOnly)

    if src_ds is None:
        raise ValueError('Could not open data set "{}"'.format(input_data_set_path))

    driver = GetDriverByName('MEM')

    src_band = src_ds.GetRasterBand(1)
    data_set = driver.Create('', src_ds.RasterXSize, src_ds.RasterYSize, 1, src_band.DataType)
    data_set.SetGeoTransform(src_ds.GetGeoTransform())
    data_set.SetProjection(src_ds.GetProjection())

    data_set_band = data_set.GetRasterBand(1)

    data_set_band.WriteArray(src_band.ReadAsArray())

    band_array = data_set_band.ReadAsArray()
    dummy = -9999
    data_set_band.SetNoDataValue(dummy)
    band_array[band_array <= -10000] = dummy
    band_array[band_array >= 10000] = dummy
    driver = GetDriverByName('GTiff')
    data_set_band.WriteArray(band_array)

    dst_ds = driver.CreateCopy(file_path, data_set, options=["COMPRESS=LZW"])

    del dst_ds
    del src_ds
    del data_set

    return file_path


def publish(collection_item: Item, scene: RadcorActivity):
    """Publish Landsat collection.

    It works with both Digital Number (DN) and Surface Reflectance (SR).

    Args:
        collection_item - Collection Item
        scene - Current Activity
    """
    identifier = scene.sceneid

    # Get collection level to publish. Default is l1
    collection_level = scene.args.get('level') or 1

    landsat_scene = factory.get_from_sceneid(identifier, level=collection_level)

    productdir = scene.args.get('file')

    logging.warning('Publish {} - {} (id={})'.format(scene.collection_id, productdir, scene.id))

    if productdir and productdir.endswith('.gz'):
        target_dir = landsat_scene.path()
        makedirs(target_dir, exist_ok=True)

        productdir = uncompress(productdir, str(target_dir))

    collection = Collection.query().filter(Collection.id == collection_item.collection_id).one()

    quicklook = Quicklook.query().filter(Quicklook.collection_id == collection.id).all()

    if quicklook:
        quicklook_bands = Band.query().filter(
            Band.id.in_(quicklook.red, quicklook.green, quicklook.blue)
        ).all()
        quicklook = [quicklook_bands[0].name, quicklook_bands[1].name, quicklook_bands[2].name]
    else:
        quicklook = DEFAULT_QUICK_LOOK_BANDS

    files = {}
    qlfiles = {}

    bands = landsat_scene.get_band_map()

    for gband, band in bands.items():
        fs = landsat_scene.get_files()

        if not fs:
            continue

        for f in fs:
            if f.stem.endswith(band) and f.suffix.lower().endswith('.tif'):
                files[gband] = f
                if gband in quicklook:
                    qlfiles[gband] = str(f)

    # Generate Vegetation Index files
    generate_vi(productdir, files)

    # Apply valid range and Cog files
    for band, file_path in files.items():
        tif_file = str(file_path)

        if landsat_scene.level == 2:
            _ = apply_valid_range(tif_file, tif_file)

        # Set destination of COG file
        files[band] = generate_cogs(tif_file, tif_file)
        if not is_valid_tif(tif_file):
            raise RuntimeError('Not Valid {}'.format(tif_file))

    # Extract basic scene information and build the quicklook
    pngname = productdir+'/{}.png'.format(identifier)

    dataset = GDALOpen(qlfiles['nir'], GA_ReadOnly)
    numlin = 768
    numcol = int(float(dataset.RasterXSize)/float(dataset.RasterYSize)*numlin)
    del dataset

    create_quick_look(pngname, [qlfiles[band] for band in quicklook if band in qlfiles], rows=numlin, cols=numcol)

    productdir = productdir.replace(Config.DATA_DIR, '')

    assets_to_upload = {
        'quicklook': dict(
            file=pngname,
            asset=productdir.replace('/Repository/Archive', '')
        )
    }

    for instance in ['local', 'aws']:
        engine_instance = {
            'local': db,
            'aws': db_aws
        }
        engine = engine_instance[instance]

        # Skip catalog on aws for digital number
        if landsat_scene.level == 1 and instance == 'aws':
            continue

        if instance == 'aws':
            asset_url = productdir.replace('/Repository/Archive', Config.AWS_BUCKET_NAME)
        else:
            asset_url = productdir

        pngname_relative = resource_path.join(asset_url, Path(pngname).name)

        assets_to_upload['quicklook']['asset'] = pngname_relative

        with engine.session.begin_nested():
            with engine.session.no_autoflush:
                # Add collection item to the session if not present
                if collection_item not in engine.session:
                    item = engine.session.query(Item).filter(
                        Item.name == collection_item.name,
                        Item.collection_id == collection_item.collection_id
                    ).first()

                    if not item:
                        cloned_properties = CollectionItemForm().dump(collection_item)
                        collection_item = Item(**cloned_properties)
                        engine.session.add(collection_item)

                # collection_item.quicklook = pngname

                collection_bands = engine.session.query(Band)\
                    .filter(Band.collection_id == collection_item.collection_id)\
                    .all()

                assets = dict(
                    thumbnail=create_asset_definition(str(asset_url), 'image/png', 'thumbnail', str(pngname))
                )

                # Inserting data into Product table
                for band in files:
                    template = resource_path.join(asset_url, Path(files[band]).name)

                    band_model = next(filter(lambda b: band == b.common_name, collection_bands), None)

                    if not band_model:
                        logging.warning('Band {} of collection {} not found in database. Skipping...'.format(
                            band, collection_item.collection_id))
                        continue

                    assets[band_model.name] = create_asset_definition(
                        template, COG_MIME_TYPE,
                        'data', files[band], is_raster=True
                    )

                    assets_to_upload[band] = dict(file=files[band], asset=template)

                collection_item.assets = assets
                # Add into scope of local and remote database
                add_instance(engine, collection_item)

        # Persist database
        commit(engine)

    return assets_to_upload
