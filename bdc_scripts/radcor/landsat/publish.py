# Python Native
import datetime
import glob
import os

# 3rdparty
from gdal import GA_ReadOnly, Open as GDALOpen
from numpngw import write_png
from ogr import osr
from skimage import exposure
from skimage.transform import resize
import numpy

# BDC Scripts
from bdc_scripts.models import db
from bdc_scripts.models import CatalogProduct, CatalogQlook, CatalogScene


def publish(scene):
    identifier = scene['sceneid']
    cc = scene['sceneid'].split('_')
    pathrow = cc[2]
    date = cc[3]
    yyyymm = cc[3][:4]+'-'+cc[3][4:6]
    # CatalogProduct dir
    productdir = scene.get('file') # '/LC8SR/{}/{}'.format(yyyymm, pathrow)

    path = int(pathrow[0:3])
    row = int(pathrow[3:])

    # Delete scene
    CatalogScene.query().filter(CatalogScene.SceneId == identifier).delete()
    # Delete products
    CatalogProduct.query().filter(CatalogProduct.SceneId == identifier).delete()
    # Delete products
    CatalogQlook.query().filter(CatalogQlook.SceneId == identifier).delete()

    # Get the product files
    bandmap= {
        'coastal': 'sr_band1',
        'blue': 'sr_band2',
        'green': 'sr_band3',
        'red': 'sr_band4',
        'nir': 'sr_band5',
        'swir1': 'sr_band6',
        'swir2': 'sr_band7',
        'evi': 'sr_evi',
        'ndvi': 'sr_ndvi',
        'quality': 'pixel_qa'
    }
    quicklook = ["swir2", "nir", "red"]

    files = {}
    qlfiles = {}
    for gband in bandmap:
        band = bandmap[gband]
        template = productdir+'/LC08_*_{}_{}_*_{}.tif'.format(pathrow,date,band)
        fs = glob.glob(template)
        files[gband] = fs[0]
        if gband in quicklook:
            qlfiles[gband] = fs[0]

    # Extract basic scene information and build the quicklook
    pngname = productdir+'/{}.png'.format(identifier)

    dataset = GDALOpen(qlfiles['nir'], GA_ReadOnly)
    numlin = 768
    numcol = int(float(dataset.RasterXSize)/float(dataset.RasterYSize)*numlin)
    image = numpy.zeros((numlin,numcol,len(qlfiles),), dtype=numpy.uint8)
    geotransform = dataset.GetGeoTransform()
    projection = dataset.GetProjection()
    datasetsrs = osr.SpatialReference()
    datasetsrs.ImportFromWkt(projection)

    # Extract bounding box and resolution
    raster_x_size = dataset.RasterXSize
    raster_y_size = dataset.RasterYSize

    del dataset

    resolutionx = geotransform[1]
    resolutiony = geotransform[5]
    fllx = fulx = geotransform[0]
    fury = fuly = geotransform[3]
    furx = flrx = fulx + resolutionx * raster_x_size
    flly = flry = fuly + resolutiony * raster_y_size

    # Create transformation from scene to ll coordinate

    llsrs = osr.SpatialReference()
    llsrs.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')
    s2ll = osr.CoordinateTransformation ( datasetsrs, llsrs )

    # Evaluate corners coordinates in ll
    # Upper left corner
    (ullon, ullat, nkulz ) = s2ll.TransformPoint( fulx, fuly)
    # Upper right corner
    (urlon, urlat, nkurz ) = s2ll.TransformPoint( furx, fury)
    # Lower left corner
    (lllon, lllat, nkllz ) = s2ll.TransformPoint( fllx, flly)
    # Lower right corner
    (lrlon, lrlat, nklrz ) = s2ll.TransformPoint( flrx, flry)

    nb = 0
    for band in quicklook:
        template = qlfiles[band]
        dataset = GDALOpen(template, GA_ReadOnly)
        raster = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)

        del dataset

        # raster = scipy.misc.imresize(raster,(numlin,numcol))
        raster = resize(raster,(numlin,numcol), order=1, preserve_range=True)
        nodata = raster == -9999
        # Evaluate minimum and maximum values
        a = numpy.array(raster.flatten())
        p1, p99 = numpy.percentile(a[a>0], (1, 99))
        # Convert minimum and maximum values to 1,255 - 0 is nodata
        raster = exposure.rescale_intensity(raster, in_range=(p1, p99),out_range=(1, 255)).astype(numpy.uint8)
        # app.logger.warning('publishLC8 - band {} p1 {} p99 {}'.format(band,p1,p99))
        image[:, :, nb] = raster.astype(numpy.uint8) * numpy.invert(nodata)
        nb += 1

    write_png(pngname, image, transparent=(0, 0, 0))

    scene_model = CatalogScene()
    scene_model.SceneId = identifier
    scene_model.Dataset = 'LC8SR'
    scene_model.Satellite = 'LC8'
    scene_model.Sensor = 'OLI'
    scene_model.Date = date
    scene_model.Path = path
    scene_model.Row = row
    scene_model.CenterLatitude = (ullat + lrlat + urlat + lllat) / 4
    scene_model.CenterLongitude = (ullon + lrlon + urlon + lllon) / 4.
    scene_model.TL_Longitude = ullon
    scene_model.TL_Latitude = ullat
    scene_model.BR_Longitude = lrlon
    scene_model.BR_Latitude = lrlat
    scene_model.TR_Longitude = urlon
    scene_model.TR_Latitude = urlat
    scene_model.BL_Longitude = lllon
    scene_model.BL_Latitude = lllat
    scene_model.IngestDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    scene_model.Deleted = 0
    scene_model.CloudCoverMethod = 'M'
    scene_model.CloudCoverQ1 = 0
    scene_model.CloudCoverQ2 = 0
    scene_model.CloudCoverQ3 = 0
    scene_model.CloudCoverQ4 = 0

    with db.session.begin_nested():
        scene_model.save(commit=False)

        # Inserting data into Qlook table
        qlook = CatalogQlook(SceneId=identifier, QLfilename=pngname)
        qlook.save(commit=False)

        # Inserting data into Product table
        for band in bandmap:
            template = files[band]

            dataset = GDALOpen(template, GA_ReadOnly)
            geotransform = dataset.GetGeoTransform()

            del dataset

            product = CatalogProduct()
            product.SceneId = identifier
            product.Type = 'SCENE'
            product.GeometricProcessing = 'ortho'
            product.RadiometricProcessing = 'SR'
            product.Dataset = 'LC8SR'
            product.Resolution = geotransform[1]
            processing_date = datetime.datetime.fromtimestamp(os.path.getctime(template)).strftime('%Y-%m-%d %H:%M:%S')
            product.Band = band
            product.Filename = template
            product.ProcessingDate = processing_date

            product.save(commit=False)
    
    db.session.commit()

    return 0