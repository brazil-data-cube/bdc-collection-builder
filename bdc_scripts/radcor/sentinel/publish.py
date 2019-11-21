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
from osgeo.osr import CoordinateTransformation, SpatialReference
from skimage.transform import resize

# BDC Scripts
from bdc_scripts.config import Config
from bdc_scripts.core.utils import generate_cogs
from bdc_scripts.models.base_sql import db
from bdc_scripts.models.catalog import CatalogProduct, CatalogScene, CatalogQlook
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


def publish(scene: RadcorActivity):
    qlband = 'TCI'

    # Retrieves all jp2 files from scene
    jp2files = get_jp2_files(scene)

    # Find the desired files to be published and put then in files
    bands = []

    files = {}
    for jp2file in sorted(jp2files):
        filename = os.path.basename(jp2file)
        parts = filename.split('_')
        band = parts[-2]

        if band not in bands and band in SENTINEL_BANDS:
            bands.append(band)
            files[BAND_MAP[band]] = jp2file
        elif band == qlband:
            files['qlfile'] = jp2file

    # Define new filenames for products
    parts = os.path.basename(files['qlfile']).split('_')
    file_basename = '_'.join(parts[:-2])

    # The sentinel directory consists in:
    # /path/to/data/S2A_MSIL2A_DATE_...*.SAFE/GRANULE/L2A_*/IMG_DATA/*/*.jp2

    # Retrieve .SAFE folder name
    scene_file_path = Path(scene.file)
    safe_filename = scene_file_path.name

    # Get year month from .SAFE folder
    year_month_part = safe_filename.split('_')[2]
    yyyymm = '{}-{}'.format(year_month_part[:4], year_month_part[4:6])

    productdir = os.path.join(Config.DATA_DIR, 'Repository/Archive/S2SR/{}/{}'.format(yyyymm, safe_filename))

    if not os.path.exists(productdir):
        os.makedirs(productdir)

    # Create vegetation index
    # app.logger.warning('Generate Vegetation index')
    generate_vi(file_basename, productdir, files)

    bands.append('NDVI')
    bands.append('EVI')

    BAND_MAP['NDVI'] = 'ndvi'
    BAND_MAP['EVI'] = 'evi'

    # Convert original format to COG
    for sband in bands:
        band = BAND_MAP[sband]
        file = files[band]

        # Set destination of COG file
        cog_file_path = os.path.join(productdir, '{}_{}.tif'.format(file_basename, sband))

        files[band] = generate_cogs(file, cog_file_path)

    # Create Qlook file
    pngname = os.path.join(productdir, file_basename + '.png')
    if not os.path.exists(pngname):
        create_qlook_file(pngname, files['qlfile'])

    store_in_database(scene_file_path.stem, pngname, files, bands)


def store_in_database(identifier: str, pngname: str, files: dict, bands: list):
    """
    Persist the generated files in database.
    It opens the quality file in order to get further metadata, such cloud coverage before insert into database.

    Args:
         identifier (str): Scene Id
         pngname (str): Quicklook file path
         files (dict): Dict of Sentinel Band files
         bands (list<str): List of bands
    """

    # Extract basic parameters from quality file
    dataset = gdal.Open(files['quality'], gdal.GA_ReadOnly)
    raster = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)
    projection = dataset.GetProjection()
    geotransform = dataset.GetGeoTransform()
    datasetsrs = SpatialReference()
    datasetsrs.ImportFromWkt(projection)

    # Get Scene from folder name
    parts = identifier.split('_')
    sat = parts[0]
    inst = parts[1][0:3]
    date = parts[2][0:8]
    calendardate = date[0:4] + '-' + date[4:6] + '-' + date[6:8]

    # Delete scene
    CatalogScene.query().filter(CatalogScene.SceneId == identifier).delete()
    # Delete products
    CatalogProduct.query().filter(CatalogProduct.SceneId == identifier).delete()
    # Delete products
    CatalogQlook.query().filter(CatalogQlook.SceneId == identifier).delete()

    llsrs = SpatialReference()
    llsrs.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')
    s2ll = CoordinateTransformation(datasetsrs, llsrs)

    # Extract bounding box and resolution
    raster_xsize = dataset.RasterXSize
    raster_ysize = dataset.RasterYSize

    resolutionx = geotransform[1]
    resolutiony = geotransform[5]
    fllx = fulx = geotransform[0]
    fury = fuly = geotransform[3]
    furx = flrx = fulx + resolutionx * raster_xsize
    flly = flry = fuly + resolutiony * raster_ysize

    # Evaluate corners coordinates in ll
    # Upper left corner
    ullon, ullat, nkulz = s2ll.TransformPoint(fulx, fuly)
    # Upper right corner
    urlon, urlat, nkurz = s2ll.TransformPoint(furx, fury)
    # Lower left corner
    lllon, lllat, nkllz = s2ll.TransformPoint(fllx, flly)
    # Lower right corner
    lrlon, lrlat, nklrz = s2ll.TransformPoint(flrx, flry)

    scene_model = CatalogScene()
    scene_model.SceneId = str(identifier)
    scene_model.Dataset = 'S2SR'
    scene_model.Satellite = sat
    scene_model.Sensor = inst
    scene_model.Date = calendardate
    scene_model.Path = 0
    scene_model.Row = 0
    scene_model.CenterLatitude = (ullat+lrlat+urlat+lllat)/4.
    scene_model.CenterLongitude = (ullon+lrlon+urlon+lllon)/4.
    scene_model.TL_LONGITUDE = ullon
    scene_model.TL_LATITUDE = ullat
    scene_model.BR_LONGITUDE = lrlon
    scene_model.BR_LATITUDE = lrlat
    scene_model.TR_LONGITUDE = urlon
    scene_model.TR_LATITUDE = urlat
    scene_model.BL_LONGITUDE = lllon
    scene_model.BL_LATITUDE = lllat
    scene_model.IngestDate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    scene_model.Deleted = 0

    # Compute cloud cover
    cloudcover = compute_cloud_cover(raster)

    scene_model.CloudCoverMethod = 'A'
    scene_model.CloudCoverQ1 = cloudcover
    scene_model.CloudCoverQ2 = cloudcover
    scene_model.CloudCoverQ3 = cloudcover
    scene_model.CloudCoverQ4 = cloudcover

    qlook = CatalogQlook(SceneId=identifier, QLfilename=pngname)

    with db.session.begin_nested():
        scene_model.save(commit=False)
        qlook.save(commit=False)

        for sband in bands:
            product = CatalogProduct()

            band = BAND_MAP[sband]
            file = files[band]

            dataset = gdal.Open(file)
            geotransform = dataset.GetGeoTransform()

            product.Dataset = 'S2SR'
            product.Type = 'SCENE'
            product.Band = band
            product.Filename = file
            product.RadiometricProcessing = 'SR'
            product.ProcessingDate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            product.GeometricProcessing = 'ortho'
            product.SceneId = identifier
            product.Resolution = geotransform[1]
            product.save(commit=False)

    db.session.commit()


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
    sentinel_folder_data = scene.file.replace('MSIL1C', 'MSIL2A')
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