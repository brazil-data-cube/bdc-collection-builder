#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Describe Sentinel 2 publish generation."""

# Python Native
from pathlib import Path
from shutil import copy
import logging
import os
# 3rdparty
import gdal
import numpy
from bdc_db.models import db, Asset, Band, CollectionItem
from numpngw import write_png
from skimage.transform import resize
# Builder
from ...config import Config
from ...db import add_instance, commit, db_aws
from ..forms import CollectionItemForm
from ..utils import get_or_create_model, generate_cogs, generate_evi_ndvi, is_valid_tif, create_quick_look
from ..models import RadcorActivity
from .utils import get_jp2_files, get_tif_files, factory


def publish(collection_item: CollectionItem, scene: RadcorActivity):
    """Publish Sentinel collection.

    It works with both L1C and L2A.

    Args:
        collection_item - Collection Item
        scene - Current Activity
    """
    qlband = 'TCI'

    # Get collection level to publish. Default is l1
    # TODO: Check in database the scenes level 2 already published. We must set to level 2
    collection_level = scene.args.get('level') or 1
    sentinel_scene = factory.get_from_sceneid(scene.sceneid, level=collection_level)
    harmonized_scene = factory.get_from_sceneid(scene.sceneid, level=3)

    product_uri = sentinel_scene.path()
    product_uri.mkdir(parents=True, exist_ok=True)

    band_map = sentinel_scene.get_band_map()

    if scene.collection_id == harmonized_scene.id:
        # Retrieves all tif files from scene
        tiffiles = get_tif_files(scene)

        # Find the desired files to be published and put then in files
        bands = []

        files = {}
        for tiffile in sorted(tiffiles):
            filename = os.path.basename(tiffile)
            parts = filename.split('_')
            band = parts[2][:-4]  # Select removing .tif extension
            if band not in bands and band in band_map.keys():
                bands.append(band)
                files[band_map[band]] = tiffile
        logging.warning('Publish {} - {} (id={}, tiffiles={})'.format(scene.collection_id,
                                                            scene.args.get('file'),
                                                            scene.id,
                                                            len(tiffiles)))
        # Define filenames for products
        parts = os.path.basename(tiffiles[0]).split('_')
        file_basename = '_'.join(parts[:-1])
        pngname = os.path.join(scene.args.get('file'), file_basename + '.png')
        copy(pngname, str(product_uri))
    else:
        # Retrieves all jp2 files from scene

        if sentinel_scene.level == 1:
            files_list = get_jp2_files(scene)
        else:
            files_list = sentinel_scene.get_files()

        # Find the desired files to be published and put then in files
        bands = []

        files = {}
        for file in sorted(files_list):
            filename = Path(file).stem
            parts = filename.split('_')

            if len(parts) in (3, 8):
                band = parts[-1]
            else:
                band = '_'.join(parts[-2:])

            if band not in bands and band in band_map.keys():
                bands.append(band)
                files[band_map[band]] = str(file)
            elif band == qlband:
                files['qlfile'] = str(file)

        logging.warning('Publish {} - {} (id={}, files={})'.format(
            scene.collection_id, scene.args.get('file'),
            scene.id, len(files)
        ))

        if len(files.keys()) == 0:
            raise RuntimeError('No files found for {} - {}'.format(scene.sceneid, str(product_uri)))

        # Retrieve a file name and use as reference for the Vegetation Index files
        file_name = Path(files.get('quality', list(files.values())[0])).name

        file_basename = '_'.join(file_name.split('_')[:-1])

    # Create vegetation index
    generate_vi(file_basename, str(product_uri), files)

    bands.append('NDVI')
    bands.append('EVI')

    band_map['NDVI'] = 'ndvi'
    band_map['EVI'] = 'evi'

    for sband in bands:
        band = band_map[sband]
        file = files[band]

        # Set destination of COG file
        cog_file_name = '{}_{}.tif'.format(file_basename, sband)
        cog_file_path = product_uri / cog_file_name

        files[band] = generate_cogs(str(file), str(cog_file_path))
        if not is_valid_tif(cog_file_path):
            raise RuntimeError('Not Valid {}'.format(cog_file_path))

    source = scene.sceneid.split('_')[0]

    assets_to_upload = {}

    for instance in ['local', 'aws']:
        engine_instance = {
            'local': db,
            'aws': db_aws
        }
        engine = engine_instance[instance]

        # Skip catalog on aws for digital number
        if sentinel_scene.level == 1 and instance == 'aws':
            continue

        if instance == 'aws':
            asset_url = Config.AWS_BUCKET_NAME / (product_uri.relative_to(Path(Config.DATA_DIR) / 'Repository/Archive'))
        else:
            asset_url = '/' / product_uri.relative_to(Config.DATA_DIR)

        collection_bands = engine.session.query(Band).filter(Band.collection_id == scene.collection_id).all()

        with engine.session.begin_nested():
            with engine.session.no_autoflush:
                # Add collection item to the session if not present
                if collection_item not in engine.session:
                    item = engine.session.query(CollectionItem).filter(CollectionItem.id == collection_item.id).first()

                    if not item:
                        cloned_properties = CollectionItemForm().dump(collection_item)
                        cloned_item = CollectionItem(**cloned_properties)
                        engine.session.add(cloned_item)

                # Convert original format to COG
                for sband in bands:
                    # Set destination of COG file
                    cog_file_name = '{}_{}.tif'.format(file_basename, sband)
                    cog_file_path = product_uri / cog_file_name

                    asset_dataset = gdal.Open(str(cog_file_path))

                    raster_band = asset_dataset.GetRasterBand(1)

                    chunk_x, chunk_y = raster_band.GetBlockSize()

                    band_model = next(filter(lambda b: b.name == sband, collection_bands), None)

                    if band_model is None:
                        logging.warning('Band {} not registered on database. Skipping'.format(sband))
                        continue

                    defaults = dict(
                        source=source,
                        url='{}/{}'.format(str(asset_url), cog_file_name),
                        raster_size_x=asset_dataset.RasterXSize,
                        raster_size_y=asset_dataset.RasterYSize,
                        raster_size_t=1,
                        chunk_size_t=1,
                        chunk_size_x=chunk_x,
                        chunk_size_y=chunk_y
                    )
                    asset, _ = get_or_create_model(
                        Asset,
                        defaults=defaults,
                        engine=engine,
                        collection_id=scene.collection_id,
                        band_id=band_model.id,
                        grs_schema_id=scene.collection.grs_schema_id,
                        tile_id=collection_item.tile_id,
                        collection_item_id=collection_item.id,
                    )
                    asset.url = defaults['url']

                    assets_to_upload[sband] = (dict(file=str(cog_file_path), asset=asset.url))

                    del asset_dataset

                # Create Qlook file
                pngname = product_uri / '{}.png'.format(file_basename)
                if not pngname.exists():
                    # When TCI band found, use it to generate quicklook
                    if files.get('qlfile'):
                        create_quick_look_from_tci(str(pngname), files['qlfile'])
                    else:
                        create_quick_look(str(pngname), [files['red'], files['green'], files['blue']])

                normalized_quicklook_path = os.path.normpath('{}/{}'.format(str(asset_url), os.path.basename(pngname.name)))
                assets_to_upload['quicklook'] = dict(asset=str(normalized_quicklook_path), file=str(pngname))

                c_item = engine.session.query(CollectionItem).filter(
                    CollectionItem.id == collection_item.id
                ).first()
                if c_item:
                    c_item.quicklook = normalized_quicklook_path
                    add_instance(engine, c_item)

        commit(engine)

    return assets_to_upload


def create_quick_look_from_tci(pngname, qlfile):
    """Create sentinel 2 quicklook."""
    image = numpy.ones((768, 768, 3,), dtype=numpy.uint8)
    dataset = gdal.Open(str(qlfile), gdal.GA_ReadOnly)
    for nb in [0, 1, 2]:
        raster = dataset.GetRasterBand(nb + 1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)
        image[:, :, nb] = resize(raster, (768, 768), order=1, preserve_range=True).astype(numpy.uint8)
        write_png(pngname, image, transparent=(0, 0, 0))


def generate_vi(identifier, productdir, files):
    """Prepare and generate Vegetation Index of Sentinel Products."""
    ndvi_name = os.path.join(productdir, identifier+"_NDVI.tif")
    evi_name = os.path.join(productdir, identifier+"_EVI.tif")
    files['ndvi'] = ndvi_name
    files['evi'] = evi_name

    generate_evi_ndvi(files['red'], files['nir'], files['blue'], evi_name, ndvi_name)

    if not is_valid_tif(ndvi_name) or not is_valid_tif(evi_name):
        raise RuntimeError('Not Valid Vegetation index file')
