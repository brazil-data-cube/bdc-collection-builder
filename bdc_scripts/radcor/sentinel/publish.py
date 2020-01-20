# Python Native
import fnmatch
import logging
import os
from datetime import datetime
from pathlib import Path

# 3rd-party
import gdal
import numpy
from numpngw import write_png
from skimage.transform import resize

# BDC Scripts
from bdc_db.models import db, Asset, Band, CollectionItem, CollectionTile
from bdc_scripts.config import Config
from bdc_scripts.db import add_instance, commit, db_aws
from bdc_scripts.core.utils import generate_cogs
from bdc_scripts.radcor.forms import CollectionItemForm
from bdc_scripts.radcor.utils import get_or_create_model
from bdc_scripts.radcor.models import RadcorActivity


BAND_MAP = {
    'B01': 'coastal',
    'B02': 'blue',
    'B03': 'green',
    'B04': 'red',
    'B05': 'redge1',
    'B06': 'redge2',
    'B07': 'redge3',
    'B08': 'bnir',
    'B8A': 'nir',
    'B09': 'wvap',
    'B10': 'cirrus',
    'B11': 'swir1',
    'B12': 'swir2',
    'SCL': 'quality'
}

SENTINEL_BANDS = BAND_MAP.keys()


def publish(collection_item: CollectionItem, scene: RadcorActivity):
    qlband = 'TCI'

    # Retrieves all jp2 files from scene
    jp2files = get_jp2_files(scene)

    # Find the desired files to be published and put then in files
    bands = []

    files = {}
    for jp2file in sorted(jp2files):
        filename = os.path.basename(jp2file)
        parts = filename.split('_')
        band = parts[-2] if scene.collection_id == 'S2SR_SEN28' else parts[-1].replace('.jp2', '')

        if band not in bands and band in SENTINEL_BANDS:
            bands.append(band)
            files[BAND_MAP[band]] = jp2file
        elif band == qlband:
            files['qlfile'] = jp2file

    # Define new filenames for products
    parts = os.path.basename(files['qlfile']).split('_')
    file_basename = '_'.join(parts[:-2])

    # Retrieve .SAFE folder name
    scene_file_path = Path(scene.args.get('file'))
    safe_filename = scene_file_path.name  # .replace('MSIL1C', 'MSIL2A')

    # Get year month from .SAFE folder
    year_month_part = safe_filename.split('_')[2]
    yyyymm = '{}-{}'.format(year_month_part[:4], year_month_part[4:6])

    product_uri = '/Repository/Archive/{}/{}/{}'.format(
        scene.collection_id, yyyymm, safe_filename)

    productdir = os.path.join(Config.DATA_DIR, product_uri[1:])

    if not os.path.exists(productdir):
        os.makedirs(productdir)

    # Create vegetation index
    generate_vi(file_basename, productdir, files)

    bands.append('NDVI')
    bands.append('EVI')

    BAND_MAP['NDVI'] = 'ndvi'
    BAND_MAP['EVI'] = 'evi'

    collection_bands = Band.query().filter(Band.collection_id == scene.collection_id).all()

    for sband in bands:
        band = BAND_MAP[sband]
        file = files[band]

        # Set destination of COG file
        cog_file_name = '{}_{}.tif'.format(file_basename, sband)
        cog_file_path = os.path.join(productdir, cog_file_name)

        files[band] = generate_cogs(file, cog_file_path)

    source = scene.sceneid.split('_')[0]

    assets_to_upload = {}

    for instance in ['local', 'aws']:
        engine_instance = {
            'local': db,
            'aws': db_aws
        }
        engine = engine_instance[instance]

        if instance == 'aws':
            asset_url = product_uri.replace('/Repository/Archive', Config.AWS_BUCKET_NAME)
        else:
            asset_url = product_uri

        with engine.session.begin_nested():
            # Add collection item to the session if not present
            if collection_item not in engine.session:
                item = engine.session.query(CollectionItem).filter(CollectionItem.id == collection_item.id).first()

                if not item:
                    cloned_properties = CollectionItemForm().dump(collection_item)
                    cloned_item = CollectionItem(**cloned_properties)
                    engine.session.add(cloned_item)

            restriction = dict(
                grs_schema_id=collection_item.grs_schema_id,
                tile_id=collection_item.tile_id,
                collection_id=collection_item.collection_id
            )
            _, _ = get_or_create_model(CollectionTile, defaults=restriction, engine=engine, **restriction)

            # Convert original format to COG
            for sband in bands:
                # Set destination of COG file
                cog_file_name = '{}_{}.tif'.format(file_basename, sband)
                cog_file_path = os.path.join(productdir, cog_file_name)

                asset_dataset = gdal.Open(cog_file_path)

                raster_band = asset_dataset.GetRasterBand(1)

                chunk_x, chunk_y = raster_band.GetBlockSize()

                band_model = next(filter(lambda b: b.name == sband, collection_bands), None)

                if band_model is None:
                    logging.warning('Band {} not registered on database. Skipping'.format(sband))
                    continue

                defaults = dict(
                    source=source,
                    url='{}/{}'.format(asset_url, cog_file_name),
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

                assets_to_upload[sband] = (dict(file=cog_file_path, asset=asset.url))

                del asset_dataset

            # Create Qlook file
            pngname = os.path.join(productdir, file_basename + '.png')
            if not os.path.exists(pngname):
                create_qlook_file(pngname, files['qlfile'])

            normalized_quicklook_path = os.path.normpath('{}/{}'.format(asset_url, os.path.basename(pngname)))
            assets_to_upload['quicklook'] = dict(asset=normalized_quicklook_path, file=pngname)

            c_item = engine.session.query(CollectionItem).filter(
                CollectionItem.id == collection_item.id
            ).first()
            if c_item:
                c_item.quicklook = normalized_quicklook_path
                add_instance(engine, c_item)

        commit(engine)

    return assets_to_upload


def create_qlook_file(pngname, qlfile):
    image = numpy.ones((768, 768, 3,), dtype=numpy.uint8)
    dataset = gdal.Open(qlfile, gdal.GA_ReadOnly)
    for nb in [0, 1, 2]:
        raster = dataset.GetRasterBand(nb + 1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)
        image[:, :, nb] = resize(raster, (768, 768), order=1, preserve_range=True).astype(numpy.uint8)
        write_png(pngname, image, transparent=(0, 0, 0))


def generate_vi(identifier, productdir, files):
    ndvi_name = os.path.join(productdir, identifier+"_NDVI.tif")
    evi_name = os.path.join(productdir, identifier+"_EVI.tif")
    files['ndvi'] = ndvi_name
    files['evi'] = evi_name

    if os.path.exists(ndvi_name) and os.path.exists(evi_name):
        logging.debug('generateVI returning 0 cause ndvi and evi exists')
        return

    data_set = gdal.Open(files['red'], gdal.GA_ReadOnly)
    raster_xsize = data_set.RasterXSize
    raster_ysize = data_set.RasterYSize
    red = data_set.GetRasterBand(1).ReadAsArray(0, 0, data_set.RasterXSize, data_set.RasterYSize).astype(numpy.float32)/10000.

    # Close data_set
    del data_set

    data_set = gdal.Open(files['nir'], gdal.GA_ReadOnly)
    nir = data_set.GetRasterBand(1).ReadAsArray(0, 0, data_set.RasterXSize, data_set.RasterYSize).astype(numpy.float32)/10000.
    # app.logger.warning('resize')
    nir = resize(nir, red.shape, order=1, preserve_range=True).astype(numpy.float32)
    # app.logger.warning('open blue band, read band')

    del data_set
    data_set = gdal.Open(files['blue'], gdal.GA_ReadOnly)
    blue = data_set.GetRasterBand(1).ReadAsArray(0, 0, data_set.RasterXSize, data_set.RasterYSize).astype(numpy.float32)/10000.

    # Create the ndvi image data_set if it not exists
    # app.logger.warning('Create the ndvi image data_set if it not exists')
    driver = gdal.GetDriverByName('GTiff')
    if not os.path.exists(ndvi_name):
        raster_ndvi = (10000 * (nir - red) / (nir + red + 0.0001)).astype(numpy.int16)
        ndvi_data_set = driver.Create(ndvi_name, raster_xsize, raster_ysize, 1, gdal.GDT_Int16, options=['COMPRESS=LZW',
                                                                                                         'TILED=YES'])
        ndvi_data_set.SetGeoTransform(data_set.GetGeoTransform())
        ndvi_data_set.SetProjection(data_set.GetProjection())
        ndvi_data_set.GetRasterBand(1).WriteArray(raster_ndvi)
        del ndvi_data_set

    # Create the evi image data set if it not exists
    if not os.path.exists(evi_name):
        evi_data_set = driver.Create(evi_name, raster_xsize, raster_ysize, 1, gdal.GDT_Int16, options=['COMPRESS=LZW',
                                                                                                       'TILED=YES'])
        raster_evi = (10000 * 2.5 * (nir - red)/(nir + 6. * red - 7.5 * blue + 1)).astype(numpy.int16)
        evi_data_set.SetGeoTransform(data_set.GetGeoTransform())
        evi_data_set.SetProjection(data_set.GetProjection())
        evi_data_set.GetRasterBand(1).WriteArray(raster_evi)
        del raster_evi
        del evi_data_set
    del data_set


def filter_jp2_files(directory, pattern):
    return [os.path.join(dirpath, f)
            for dirpath, dirnames, files in os.walk("{0}".format(directory))
            for f in fnmatch.filter(files, pattern)]


def get_jp2_files(scene: RadcorActivity):
    # Find all jp2 files in L2A SAFE
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


def compute_cloud_cover(raster):
    """
    Label Classification
    0      NO_DATA
    1      SATURATED_OR_DEFECTIVE
    2      DARK_AREA_PIXELS
    3      CLOUD_SHADOWS
    4      VEGETATION
    5      BARE_SOILS
    6      WATER
    7      CLOUD_LOW_PROBABILITY
    8      CLOUD_MEDIUM_PROBABILITY
    9      CLOUD_HIGH_PROBABILITY
    10     THIN_CIRRUS
    11     SNOW
    """
    unique, counts = numpy.unique(raster, return_counts=True)
    clear = 0.
    cloud = 0.
    for i in range(0, unique.shape[0]):
        if unique[i] == 0:
            continue
        elif unique[i] in [1, 2, 3, 8, 9, 10]:
            cloud += float(counts[i])
        else:
            clear += float(counts[i])

    return int(round(100. * cloud / (clear + cloud), 0))