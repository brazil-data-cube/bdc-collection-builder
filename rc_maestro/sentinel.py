import datetime
import fnmatch
import os
import time

import gdal
import numpy
import osr
from skimage.transform import resize
from numpngw import write_png
import sqlalchemy


def publish(scene):
    sbands = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B09', 'B10', 'B11', 'B12', 'SCL']
    qlband = 'TCI'
    bandmap = {
        "B01": "coastal",
        "B02": "blue",
        "B03": "green",
        "B04": "red",
        "B05": "redge1",
        "B06": "redge2",
        "B07": "redge3",
        "B08": "bnir",
        "B8A": "nir",
        "B09": "wvap",
        "B10": "cirrus",
        "B11": "swir1",
        "B12": "swir2",
        "SCL": "quality"
    }
    # Basic information about scene
    # S2B_MSIL1C_20180731T131239_N0206_R138_T24MTS_20180731T182838
    scene_id = os.path.basename(scene['file'])
    parts = scene_id.split('_')
    sat = parts[0]
    inst = parts[1][0:3]
    date = parts[2][0:8]
    calendar_date = '{}-{}-{}'.format(date[0:4], date[4:6], date[6:8])
    identifier = scene_id.split('.')[0].replace('MSIL1C', 'MSIL2A')

    # Create metadata structure and start filling metadata structure for tables Scene and Product in Catalogo database
    result = {'Scene': {}, 'Product': {}}
    result['Scene']['SceneId'] = str(identifier)
    result['Scene']['Dataset'] = 'S2SR'
    result['Scene']['Satellite'] = sat
    result['Scene']['Sensor'] = inst
    result['Scene']['Date'] = calendar_date
    result['Scene']['Path'] = 0
    result['Scene']['Row'] = 0
    result['Product']['Dataset'] = 'S2SR'
    result['Product']['Type'] = 'SCENE'
    result['Product']['RadiometricProcessing'] = 'SR'
    result['Product']['ProcessingDate'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    result['Product']['GeometricProcessing'] = 'ortho'
    result['Product']['SceneId'] = str(identifier)

    # Find all jp2 files in L2A SAFE
    safe_L2A_full = scene['file'].replace('MSIL1C', 'MSIL2A')
    template = "T*.jp2"
    jp2files = [os.path.join(dirpath, f)
                for dirpath, dirnames, files in os.walk("{0}".format(safe_L2A_full))
                for f in fnmatch.filter(files, template)]
    if len(jp2files) <= 1:
        # app.logger.warning('publishS2 - No {} files found in {}'.format(template, safe_L2A_full))
        template = "L2A_T*.jp2"
        jp2files = [os.path.join(dirpath, f)
                    for dirpath, dirnames, files in os.walk("{0}".format(safe_L2A_full))
                    for f in fnmatch.filter(files, template)]
        if len(jp2files) <= 1:
            # app.logger.warning('publishS2 - No {} files found in {}'.format(template, safe_L2A_full))
            return 1
    # app.logger.warning(
    #     'publishS2 - safeL2Afull {} found {} files template {}'.format(safe_L2A_full, len(jp2files), template))

    # Find the desired files to be published and put then in files
    bands = []
    files = {}
    for jp2file in sorted(jp2files):
        filename = os.path.basename(jp2file)
        parts = filename.split('_')
        band = parts[-2]
        if band not in bands and band in sbands:
            bands.append(band)
            files[bandmap[band]] = jp2file
        elif band == qlband:
            files['qlfile'] = jp2file

    # Define new filenames for products
    # app.logger.warning('publishS2 - qlfile {}'.format(files['qlfile']))
    parts = os.path.basename(files['qlfile']).split('_')
    filebasename = '_'.join(parts[:-2])
    parts = files['qlfile'].split('/')
    productdir = '/'.join(parts[:-2])

    # Create vegetation index
    # app.logger.warning('Generate Vegetation index')
    if generateVI(filebasename, productdir, files) != 0:
        # app.logger.warning('Vegetation index != 0')
        return 1
    bands.append('NDVI')
    bands.append('EVI')
    bandmap['NDVI'] = 'ndvi'
    bandmap['EVI'] = 'evi'

    # Convert original format to COG
    productdir = '/'.join(parts[:4])
    productdir += '/PUBLISHED'
    if not os.path.exists(productdir):
        os.makedirs(productdir)
    for sband in bands:
        band = bandmap[sband]
        file = files[band]
        # app.logger.warning('publishS2 - COG band {} sband {} file {}'.format(band,sband,file))
        files[band] = publishAsCOG(filebasename, productdir, sband, file)

    # Create Qlook file
    qlfile = files['qlfile']
    pngname = os.path.join(productdir, filebasename + '.png')
    # app.logger.warning('publishS2 - pngname {}'.format(pngname))
    if not os.path.exists(pngname):
        image = numpy.ones((768, 768, 3,), dtype=numpy.uint8)
        dataset = gdal.Open(qlfile, gdal.GA_ReadOnly)
        for nb in [0, 1, 2]:
            raster = dataset.GetRasterBand(nb + 1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)
            image[:, :, nb] = resize(raster, (768, 768), order=1, preserve_range=True).astype(numpy.uint8)
            write_png(pngname, image, transparent=(0, 0, 0))
    qlfile = pngname

    # Extract basic parameters from quality file
    file = files['quality']
    dataset = gdal.Open(file, gdal.GA_ReadOnly)
    raster = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)
    geotransform = dataset.GetGeoTransform()
    projection = dataset.GetProjection()
    datasetsrs = osr.SpatialReference()
    datasetsrs.ImportFromWkt(projection)

    # Extract bounding box and resolution
    # app.logger.warning('extract bb and resolution')
    raster_x_size = dataset.RasterXSize
    raster_y_size = dataset.RasterYSize

    resolution_x = geotransform[1]
    resolution_y = geotransform[5]
    fllx = fulx = geotransform[0]
    fury = fuly = geotransform[3]
    furx = flrx = fulx + resolution_x * raster_x_size
    flly = flry = fuly + resolution_y * raster_y_size

    # Create transformation from files to ll coordinate
    llsrs = osr.SpatialReference()
    llsrs.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')
    s2ll = osr.CoordinateTransformation(datasetsrs, llsrs)

    # Evaluate corners coordinates in ll
    # Upper left corner
    (ullon, ullat, nkulz) = s2ll.TransformPoint(fulx, fuly)
    # Upper right corner
    (urlon, urlat, nkurz) = s2ll.TransformPoint(furx, fury)
    # Lower left corner
    (lllon, lllat, nkllz) = s2ll.TransformPoint(fllx, flly)
    # Lower right corner
    (lrlon, lrlat, nklrz) = s2ll.TransformPoint(flrx, flry)

    result['Scene']['CenterLatitude'] = (ullat + lrlat + urlat + lllat) / 4.
    result['Scene']['CenterLongitude'] = (ullon + lrlon + urlon + lllon) / 4.

    result['Scene']['TL_LONGITUDE'] = ullon
    result['Scene']['TL_LATITUDE'] = ullat

    result['Scene']['BR_LONGITUDE'] = lrlon
    result['Scene']['BR_LATITUDE'] = lrlat

    result['Scene']['TR_LONGITUDE'] = urlon
    result['Scene']['TR_LATITUDE'] = urlat

    result['Scene']['BL_LONGITUDE'] = lllon
    result['Scene']['BL_LATITUDE'] = lllat

    result['Scene']['IngestDate'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    result['Scene']['Deleted'] = 0

    # Compute cloud cover
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
    cloudcover = int(round(100. * cloud / (clear + cloud), 0))
    # app.logger.warning('publishS2 - cloudcover {}'.format(cloudcover))

    result['Scene']['CloudCoverMethod'] = 'A'
    result['Scene']['CloudCoverQ1'] = cloudcover
    result['Scene']['CloudCoverQ2'] = cloudcover
    result['Scene']['CloudCoverQ3'] = cloudcover
    result['Scene']['CloudCoverQ4'] = cloudcover

    # Connect to db and delete all data about this scene
    connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('CATALOG_USER'),
                                              os.environ.get('CATALOG_PASS'),
                                              os.environ.get('CATALOG_HOST'),
                                              'catalogo')
    engine = sqlalchemy.create_engine(connection)
    sql = "DELETE FROM Scene WHERE SceneId = '{0}'".format(identifier)
    engine.execute(sql)
    sql = "DELETE FROM Product WHERE SceneId = '{0}'".format(identifier)
    engine.execute(sql)
    sql = "DELETE FROM Qlook WHERE SceneId = '{0}'".format(identifier)
    engine.execute(sql)

    # Inserting data into Scene table
    params = ''
    values = ''
    for key, val in result['Scene'].items():
        params += key + ','
        if type(val) is str:
            values += "'{0}',".format(val)
        else:
            values += "{0},".format(val)

    sql = "INSERT INTO Scene ({0}) VALUES({1})".format(params[:-1], values[:-1])
    # app.logger.warning('publishS2 - sql {}'.format(sql))
    engine.execute(sql)

    # Inserting data into Qlook table
    sql = "INSERT INTO Qlook (SceneId,QLfilename) VALUES('%s', '%s')" % (identifier, qlfile)
    # app.logger.warning('publishS2 - sql {}'.format(sql))
    engine.execute(sql)

    # Inserting data into Product table
    for sband in bands:
        band = bandmap[sband]
        file = files[band]
        ProcessingDate = datetime.datetime.fromtimestamp(os.path.getctime(file)).strftime('%Y-%m-%d %H:%M:%S')
        result['Product']['ProcessingDate'] = ProcessingDate
        dataset = gdal.Open(file, gdal.GA_ReadOnly)
        if dataset is None:
            # app.logger.warning('publishS2 - file {} is corrupted'.format(file))
            continue
        geotransform = dataset.GetGeoTransform()
        result['Product']['Resolution'] = geotransform[1]
        result['Product']['Band'] = band
        result['Product']['Filename'] = file
        params = ''
        values = ''
        for key, val in result['Product'].items():
            params += key + ','
            if type(val) is str:
                values += "'{0}',".format(val)
            else:
                values += "{0},".format(val)
        sql = "INSERT INTO Product ({0}) VALUES({1})".format(params[:-1], values[:-1])
        # app.logger.warning('publishS2 - sql {}'.format(sql))
        engine.execute(sql)
    engine.dispose()


def generateVI(identifier,productdir,files):
    ndviname = os.path.join(productdir,identifier+"_NDVI.tif")
    eviname = os.path.join(productdir,identifier+"_EVI.tif")
    # app.logger.warning('generateVI - ndviname {}'.format(ndviname))
    # app.logger.warning('generateVI - eviname {}'.format(eviname))
    # app.logger.warning('generateVI - nir {}'.format(files['nir']))
    # app.logger.warning('generateVI - red {}'.format(files['red']))
    # app.logger.warning('generateVI - blue {}'.format(files['blue']))
    files['ndvi'] = ndviname
    files['evi'] = eviname
    if os.path.exists(ndviname) and os.path.exists(eviname):
        # app.logger.warning('generateVI returning 0 cause ndvi and evi exists')
        return 0

    # app.logger.warning('open red band, read band')
    step_start = time.time()
    dataset = gdal.Open(files['red'], gdal.GA_ReadOnly)
    RasterXSize = dataset.RasterXSize
    RasterYSize = dataset.RasterYSize
    red = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize).astype(numpy.float32)/10000.
    # app.logger.warning('open nir band, read band')

    del dataset
    dataset = gdal.Open(files['nir'], gdal.GA_ReadOnly)
    nir = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize).astype(numpy.float32)/10000.
    # app.logger.warning('resize')
    nir = resize(nir,red.shape, order=1, preserve_range=True).astype(numpy.float32)
    # app.logger.warning('open blue band, read band')

    del dataset
    dataset = gdal.Open(files['blue'], gdal.GA_ReadOnly)
    blue = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize).astype(numpy.float32)/10000.

    # Create the ndvi image dataset if it not exists
    # app.logger.warning('Create the ndvi image dataset if it not exists')
    driver = gdal.GetDriverByName('GTiff')
    if not os.path.exists(ndviname):
        rasterndvi = (10000 * (nir - red) / (nir + red + 0.0001)).astype(numpy.int16)
        # rasterndvi[rasterndvi<=0] = 0
        # app.logger.warning('generateVI - ndviname {} shape {} {} {}'.format(ndviname,rasterndvi.shape,
        #                                                                     dataset.RasterXSize,
        #                                                                     dataset.RasterYSize))
        ndvidataset = driver.Create( ndviname, RasterXSize, RasterYSize, 1, gdal.GDT_Int16, options=['COMPRESS=LZW',
                                                                                                     'TILED=YES'])
        ndvidataset.SetGeoTransform(dataset.GetGeoTransform())
        ndvidataset.SetProjection(dataset.GetProjection())
        ndvidataset.GetRasterBand(1).WriteArray( rasterndvi )
        # ndvidataset.GetRasterBand(1).SetNoDataValue(0)
        del ndvidataset

    # Create the evi image dataset if it not exists
    # app.logger.warning('Create the evi image dataset if it not exists')
    if not os.path.exists(eviname):
        evidataset = driver.Create( eviname, RasterXSize, RasterYSize, 1, gdal.GDT_Int16,  options = [ 'COMPRESS=LZW', 'TILED=YES'  ] )
        rasterevi = (10000 * 2.5 * (nir - red)/(nir + 6. * red - 7.5 * blue + 1)).astype(numpy.int16)
        # app.logger.warning('generateVI - eviname {} shape {} {} {}'.format(eviname,rasterevi.
        #                                                                    shape,dataset.RasterXSize,
        #                                                                    dataset.RasterYSize))
        # rasterevi[rasterevi<=0] = 0
        evidataset.SetGeoTransform(dataset.GetGeoTransform())
        evidataset.SetProjection(dataset.GetProjection())
        evidataset.GetRasterBand(1).WriteArray( rasterevi )
        # evidataset.GetRasterBand(1).SetNoDataValue(0)
        rasterevi = None
        evidataset = None
    dataset = nir = red = blue = None
    elapsedtime = time.time() - step_start
    ela = str(datetime.timedelta(seconds=elapsedtime))
    # app.logger.warning('create VI returning 0 Ok')
    return 0


def publishAsCOG(identifier,productdir,sband,jp2file,alreadyTiled=False):
    # app.logger.warning('function:publishAsCOG')
    cogfile = os.path.join(productdir,identifier+'_'+sband+'.tif')
    if os.path.exists(cogfile):
        return cogfile
    driver = gdal.GetDriverByName('GTiff')

    dataset = gdal.Open(jp2file, gdal.GA_ReadOnly)
    dst_ds = driver.CreateCopy(cogfile, dataset,  options=['COMPRESS=LZW', 'TILED=YES'])
    gdal.SetConfigOption('COMPRESS_OVERVIEW', 'LZW')
    dst_ds.BuildOverviews('NEAREST', [2, 4, 8, 16, 32])

    del dst_ds
    del dataset

    return cogfile