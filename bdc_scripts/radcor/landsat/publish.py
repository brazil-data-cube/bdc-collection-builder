from bdc_scripts.models import db
from bdc_scripts.radcor.models import Product, Qlook, Scene


def publish(scene):
    identifier = scene['sceneid']
    cc = scene['sceneid'].split('_')
    pathrow = cc[2]
    date = cc[3]
    yyyymm = cc[3][:4]+'-'+cc[3][4:6]
    # Product dir
    productdir = '/LC8SR/{}/{}'.format(yyyymm, pathrow)
    Type='SCENE'
    GeometricProcessing='ortho'
    RadiometricProcessing='SR'
    path = int(pathrow[0:3])
    row  = int(pathrow[3:])

    # Delete scene
    Scene.query().filter(Scene.sceneid == identifier).delete()
    # Delete products
    Product.query().filter(Product.sceneid == identifier).delete()
    # Delete products
    Qlook.query().filter(Qlook.sceneid == identifier).delete()

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
    quicklook = ["swir2","nir","red"]

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
    pngexists = False
    if os.path.exists(pngname):
        pngexists = True

    dataset = gdal.Open(qlfiles['nir'],GA_ReadOnly)
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
    #	Upper left corner
    (ullon, ullat, nkulz ) = s2ll.TransformPoint( fulx, fuly)
    #	Upper right corner
    (urlon, urlat, nkurz ) = s2ll.TransformPoint( furx, fury)
    #	Lower left corner
    (lllon, lllat, nkllz ) = s2ll.TransformPoint( fllx, flly)
    #	Lower right corner
    (lrlon, lrlat, nklrz ) = s2ll.TransformPoint( flrx, flry)

    product = Product()
    product.sceneid = identifier
    product.type = 'SCENE'

    scene_model = Scene()
    scene_model.sceneid = identifier
    scene_model.dataset = 'LC8SR'
    scene_model.satellite = 'LC8'
    scene_model.date = date

    result = {'Scene':{},'Product':{}}
    result['Scene']['SceneId'] = identifier
    result['Scene']['Dataset'] = 'LC8SR'
    result['Scene']['Satellite'] = 'LC8'
    result['Scene']['Sensor'] = 'OLI'
    result['Scene']['Date'] = date
    result['Scene']['Path'] = path
    result['Scene']['Row'] = row

    result['Product']['SceneId'] = identifier
    result['Product']['Dataset'] = 'LC8SR'
    result['Product']['Type'] = 'SCENE'
    result['Product']['GeometricProcessing'] = GeometricProcessing
    result['Product']['RadiometricProcessing'] = RadiometricProcessing

    result['Scene']['CenterLatitude'] = (ullat+lrlat+urlat+lllat)/4.
    result['Scene']['CenterLongitude'] = (ullon+lrlon+urlon+lllon)/4.

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
    result['Scene']['CloudCoverMethod'] = 'M'
    result['Scene']['CloudCoverQ1'] = 0
    result['Scene']['CloudCoverQ2'] = 0
    result['Scene']['CloudCoverQ3'] = 0
    result['Scene']['CloudCoverQ4'] = 0

    nb = 0
    for band in quicklook:
        template = qlfiles[band]
        dataset = gdal.Open(template,GA_ReadOnly)
        raster = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)

        del dataset

        app.logger.warning('publishLC8 - file {} raster before min {} max {} {}'.format(template,raster.min(),raster.max(),raster))
        #raster = scipy.misc.imresize(raster,(numlin,numcol))
        raster = resize(raster,(numlin,numcol), order=1, preserve_range=True)
        nodata = raster == -9999
        app.logger.warning('publishLC8 - file {} raster after min {} max {} {}'.format(template,raster.min(),raster.max(),raster))
      # Evaluate minimum and maximum values
        a = numpy.array(raster.flatten())
        p1, p99 = numpy.percentile(a[a>0], (1, 99))
      # Convert minimum and maximum values to 1,255 - 0 is nodata
        raster = exposure.rescale_intensity(raster, in_range=(p1, p99),out_range=(1,255)).astype(numpy.uint8)
        #app.logger.warning('publishLC8 - band {} p1 {} p99 {}'.format(band,p1,p99))
        image[:,:,nb] = raster.astype(numpy.uint8) * numpy.invert(nodata)
        nb += 1

    write_png(pngname, image, transparent=(0, 0, 0))

    # Inserting data into Scene table
    params = ''
    values = ''
    for key,val in result['Scene'].items():
        params += key+','
        if type(val) is str:
            values += "'{0}',".format(val)
        else:
            values += "{0},".format(val)

    sql = "INSERT INTO Scene ({0}) VALUES({1})".format(params[:-1],values[:-1])
    engine.execute(sql)

    with db.session.begin_nested():
        # Inserting data into Qlook table
        qlook = Qlook(sceneid=identifier, qlookfile=pngname)
        qlook.save(False)

        # Inserting data into Product table
        for band in bandmap:
            template = files[band]

            dataset = gdal.Open(template,GA_ReadOnly)
            geotransform = dataset.GetGeoTransform()

            del dataset

            result['Product']['Resolution'] = geotransform[1]
            processing_date = datetime.datetime.fromtimestamp(os.path.getctime(template)).strftime('%Y-%m-%d %H:%M:%S')

            product.band = band
            product.filename = template
            product.processingdate = processing_date

            product.save(False)
    
    db.session.commit()

    return 0