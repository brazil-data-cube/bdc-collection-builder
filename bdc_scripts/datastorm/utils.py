# Python Native
from datetime import date, timedelta
import os
import time
# 3rdparty
from geoalchemy2 import func
from osgeo import osr
from rasterio import Affine, MemoryFile
from rasterio.warp import reproject, Resampling
import gdal
import numpy
import rasterio
# BDC Scripts
from bdc_db.models import Band, Collection, db, Tile
from bdc_scripts.config import Config


PROJ4 = {
    "aea_250k": "+proj=aea +lat_1=10 +lat_2=-40 +lat_0=0 +lon_0=-50 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
    "aea_500k": "+proj=aea +lat_1=10 +lat_2=-40 +lat_0=0 +lon_0=-50 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
}


args = dict(
    links=[],
    resx=10,
    resy=10,
    cols=15727,
    rows=11523,
    band='blue',
    nodata=0,
    srs='+proj'
)


def warp(link: str, tile_id: str, band_meta: dict, datacube: str, scene_date: date, mgrs: str):
    # Get the input and warped files name
    filename = '/vsicurl/' + link

    datacube_warped = datacube

    for fn in ['MEDIAN', 'STACK']:
        datacube_warped = datacube_warped.replace(fn, 'WARPED')

    warped = os.path.join(Config.DATA_DIR, 'Repository/collections/archive/warped/{}/{}_{}_{}_{}.tif'.format(datacube_warped, tile_id, mgrs, scene_date.strftime('%Y-%m-%d'), band_meta['common_name']))

    cube = Collection.query().filter(Collection.id == datacube).first()
    cube_warped = Collection.query().filter(Collection.id == datacube_warped).first()
    raster_schema = cube.raster_size_schemas
    band = Band.query().filter(Band.collection_id == datacube, Band.common_name == band_meta['common_name']).first()

    query_geom_origin = db.session.query(
        Tile.grs_schema_id,
        func.ST_Transform(
            func.ST_SetSRID(Tile.geom_wgs84, 4326),
            '+proj=aea +lat_1=10 +lat_2=-40 +lat_0=0 +lon_0=-50 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs'
        ).label('geom_origin')
        # Tile.geom_wgs84.label('geom_origin')
    ).filter(Tile.id == tile_id).subquery()

    tile = db.session.query(
        query_geom_origin.c.grs_schema_id,
        func.ST_XMin(query_geom_origin.c.geom_origin),
        func.ST_YMax(query_geom_origin.c.geom_origin)
    ).first()

    scenesrs = osr.SpatialReference()
    scenesrs.ImportFromProj4(PROJ4[tile[0]])

    # If warped file not exists, re-project the input scene
    if not os.path.exists(warped):
        src_ds = gdal.Open(filename)

        if src_ds is None:
            raise IOError('Dataset not found "{}".'.format(filename))

        warped_dir = os.path.dirname(warped)
        os.makedirs(warped_dir, exist_ok=True)

        src_ds.GetRasterBand(1).SetNoDataValue(0)

        # Now, we create an in-memory raster
        mem_drv = gdal.GetDriverByName('MEM')
        tmp_ds = mem_drv.Create('', int(raster_schema.raster_size_x), int(raster_schema.raster_size_y), 1, gdal.GDT_UInt16)

        # Set the geotransform
        tmp_ds.SetGeoTransform([tile[1], int(band.resolution_x), 0, tile[2], 0, -int(band.resolution_y)])
        tmp_ds.SetProjection(scenesrs.ExportToWkt())
        tmp_ds.GetRasterBand(1).SetNoDataValue(0)

        # Perform the projection/resampling
        if band.common_name == 'quality':
            resampling = gdal.GRA_NearestNeighbour
        else:
            resampling = gdal.GRA_Bilinear
        error_threshold = 0.125
        try:
            res = gdal.ReprojectImage(src_ds, tmp_ds, 'PROJCS["WGS 84 / UTM zone 23S",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",10000000],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","32723"]]', tmp_ds.GetProjection(), resampling)
        except BaseException as e:
            raise RuntimeError('Could not reproject image - {}'.format(e))

        # Create the final warped raster
        driver = gdal.GetDriverByName('GTiff')
        dst_ds = driver.CreateCopy(warped, tmp_ds, options=['COMPRESS=LZW', 'TILED=YES'])
        dst_ds = None
        tmp_ds = None

    # if band is quality lets evaluate the mask stats for the scene
    if band.common_name == 'quality':
        maskfile = warped.replace('quality.tif','mask.tif')
        dataset = scene['dataset']
        mask = getMask(warped,dataset)

        if mask is None:
            raise FileNotFoundError('No mask {}'.format(maskfile))

        cloudratio, clearratio, efficacy = getMaskStats(mask)
        # nscene = {}
        # nscene['cloudratio'] = cloudratio
        # nscene['clearratio'] = clearratio
        # nscene['efficacy'] = efficacy
        if efficacy <= 0.1:
            return 0,'Efficacy {} is too low for {}'.format(efficacy,filename)

    return 0,'Normal execution'


def merge(datacube, tile_id, assets, cols, rows, period, **kwargs):
    nodata = kwargs.get('nodata', None)
    xmin = kwargs.get('xmin')
    ymax = kwargs.get('ymax')
    dataset = kwargs.get('dataset')
    band = assets[0]['band']
    merge_date = kwargs.get('date')

    srs = kwargs.get('srs', '+proj=aea +lat_1=10 +lat_2=-40 +lat_0=0 +lon_0=-50 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs')

    merged_file = os.path.join(Config.DATA_DIR, 'Repository/collections/cubes/{}/{}/{}/{}_{}.tif'.format(datacube, tile_id, period, band, merge_date))

    resx, resy = assets[0]['resolution_x'], assets[0]['resolution_y']

    transform = Affine(resx, 0, xmin, 0, -resy, ymax)

    # Quality band is resampled by nearest, other are bilinear
    if band == 'quality':
        resampling = Resampling.nearest
    else:
        resampling = Resampling.bilinear

    # For all files
    src = rasterio.open(assets[0]['link'])
    raster = numpy.zeros((rows, cols,), dtype=src.profile['dtype'])
    rasterMerge = numpy.zeros((rows, cols,), dtype=src.profile['dtype'])
    rasterMask = numpy.ones((rows, cols,), dtype=src.profile['dtype'])
    count = 0
    template = None
    for asset in assets:
        count += 1
        step_start = time.time()
        with rasterio.Env(CPL_CURL_VERBOSE=False):
            with rasterio.open(asset['link']) as src:
                kwargs = src.meta.copy()
                kwargs.update({
                    'crs': srs,
                    'transform': transform,
                    'width': cols,
                    'height': rows
                })

                if src.profile['nodata'] is not None:
                    nodata = src.profile['nodata']
                elif nodata is None:
                    nodata = 0

                kwargs.update({
                    'nodata': nodata
                })

                with MemoryFile() as memfile:
                    with memfile.open(**kwargs) as dst:
                        reproject(
                            source=rasterio.band(src, 1),
                            destination=raster,
                            src_transform=src.transform,
                            src_crs=src.crs,
                            dst_transform=transform,
                            dst_crs=srs,
                            src_nodata=nodata,
                            dst_nodata=nodata,
                            resampling=resampling)
                        rasterMerge = rasterMerge + raster*rasterMask
                        rasterMask[raster!=nodata] = 0
                        if template is None:
                            template = dst.profile
                elapsedtime = time.time() - step_start
                ela = str(timedelta(seconds=elapsedtime))

    # Evaluate cloud cover and efficacy if band is quality
    efficacy = 0
    cloudratio = 100
    if band == 'quality':
        rasterMerge, efficacy, cloudratio = getMask(rasterMerge, dataset)
        template.update({'dtype': 'uint8'})

    target_dir = os.path.dirname(merged_file)
    os.makedirs(target_dir, exist_ok=True)

    with rasterio.open(merged_file, 'w', **template) as merge_dataset:
        merge_dataset.write_band(1, rasterMerge)

    # Update entry in DynamoDB
    # activity['efficacy'] = '{}'.format(int(efficacy))
    # activity['cloudratio'] = '{}'.format(int(cloudratio))

    # self.S3client.put_object(Bucket=self.bucket_name, Key=key,Body=(bytes(json.dumps(activity).encode('UTF-8'))))
    # elapsedtime = time.time() - program_start
    # ela = str(timedelta(seconds=elapsedtime))


def blend(scene):
    pass


def publish(scene):
    pass


def getMask(raster, dataset):
    from skimage import morphology
    # Output Cloud Mask codes
    # 0 - fill
    # 1 - clear data
    # 0 - cloud
    if dataset == 'LC8SR':
        # Input pixel_qa codes
        fill    = 1 				# warped images have 0 as fill area
        terrain = 2					# 0000 0000 0000 0010
        radsat  = 4+8				# 0000 0000 0000 1100
        cloud   = 16+32+64			# 0000 0000 0110 0000
        shadow  = 128+256			# 0000 0001 1000 0000
        snowice = 512+1024			# 0000 0110 0000 0000
        cirrus  = 2048+4096			# 0001 1000 0000 0000

        unique, counts = numpy.unique(raster, return_counts=True)

        # Start with a zeroed image imagearea
        imagearea = numpy.zeros(raster.shape, dtype=numpy.bool_)
        # Mark with True the pixels that contain valid data
        imagearea = imagearea + raster > fill
        # Create a notcleararea mask with True where the quality criteria is as follows
        notcleararea = 	(raster & radsat > 4) + \
                    (raster & cloud > 64) + \
                    (raster & shadow > 256) + \
                    (raster & snowice > 512) + \
                    (raster & cirrus > 4096)

        strel = morphology.selem.square(6)
        notcleararea = morphology.binary_dilation(notcleararea,strel)
        morphology.remove_small_holes(notcleararea, area_threshold=80, connectivity=1, in_place=True)

        # Clear area is the area with valid data and with no Cloud or Snow
        cleararea = imagearea * numpy.invert(notcleararea)
        # Code the output image rastercm as the output codes
        rastercm = (2*notcleararea + cleararea).astype(numpy.uint8)

    elif dataset == 'MOD13Q1' or dataset == 'MYD13Q1':
        # MOD13Q1 Pixel Reliability !!!!!!!!!!!!!!!!!!!!
        # Note that 1 was added to this image in downloadModis because of warping
        # Rank/Key Summary QA 		Description
        # -1 		Fill/No Data 	Not Processed
        # 0 		Good Data 		Use with confidence
        # 1 		Marginal data 	Useful, but look at other QA information
        # 2 		Snow/Ice 		Target covered with snow/ice
        # 3 		Cloudy 			Target not visible, covered with cloud
        fill    = 0 	# warped images have 0 as fill area
        lut = numpy.array([0,1,1,2,2],dtype=numpy.uint8)
        rastercm = numpy.take(lut,raster+1).astype(numpy.uint8)

    elif dataset == 'S2SR':
        # S2 sen2cor - The generated classification map is specified as follows:
        # Label Classification
        #  0		NO_DATA
        #  1		SATURATED_OR_DEFECTIVE
        #  2		DARK_AREA_PIXELS
        #  3		CLOUD_SHADOWS
        #  4		VEGETATION
        #  5		NOT_VEGETATED
        #  6		WATER
        #  7		UNCLASSIFIED
        #  8		CLOUD_MEDIUM_PROBABILITY
        #  9		CLOUD_HIGH_PROBABILITY
        # 10		THIN_CIRRUS
        # 11		SNOW
        # 0 1 2 3 4 5 6 7 8 9 10 11
        lut = numpy.array([0,0,2,2,1,1,1,2,2,2,1, 1],dtype=numpy.uint8)
        rastercm = numpy.take(lut,raster).astype(numpy.uint8)

    elif dataset == 'CB4_AWFI' or dataset == 'CB4_MUX':
        # Key 		Summary QA 		Description
        # 0 		Fill/No Data 	Not Processed
        # 127 		Good Data 		Use with confidence
        # 255 		Cloudy 			Target not visible, covered with cloud
        fill = 0 		# warped images have 0 as fill area
        lut = numpy.zeros(256,dtype=numpy.uint8)
        lut[127] = 1
        lut[255] = 2
        rastercm = numpy.take(lut,raster).astype(numpy.uint8)

    unique, counts = numpy.unique(rastercm, return_counts=True)

    totpix   = rastercm.size
    fillpix  = numpy.count_nonzero(rastercm==0)
    clearpix = numpy.count_nonzero(rastercm==1)
    cloudpix = numpy.count_nonzero(rastercm==2)
    imagearea = clearpix+cloudpix
    clearratio = 0
    cloudratio = 100
    if imagearea != 0:
        clearratio = round(100.*clearpix/imagearea,1)
        cloudratio = round(100.*cloudpix/imagearea,1)
    efficacy = round(100.*clearpix/totpix,2)

    return rastercm,efficacy,cloudratio