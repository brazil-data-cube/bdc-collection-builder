# Python Native
from datetime import date
import os
# 3rdparty
from geoalchemy2 import func
from osgeo import osr
import gdal
# BDC Scripts
from bdc_db.models import Band, Collection, db, Tile
from bdc_scripts.config import Config


PROJ4 = {
    "aea_250k": "+proj=aea +lat_1=10 +lat_2=-40 +lat_0=0 +lon_0=-50 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
    "aea_500k": "+proj=aea +lat_1=10 +lat_2=-40 +lat_0=0 +lon_0=-50 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
}


def warp(link: str, scene_id: str, tile_id: str, band_meta: dict, datacube: str, scene_date: date, mgrs: str):
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
            return 1,'No mask {}'.format(maskfile)
        (cloudratio,clearratio,efficacy) = getMaskStats(mask)
        nscene = {}
        nscene['cloudratio'] = cloudratio
        nscene['clearratio'] = clearratio
        nscene['efficacy'] = efficacy
        if efficacy <= 0.1:
            return 0,'Efficacy {} is too low for {}'.format(efficacy,filename)

        params = ''
        for key,val in nscene.items():
            params += "{} = {},".format(key,val)
        # Update all bands with cloud information
        sql = "UPDATE scenes SET {} WHERE sceneid = '{}' AND datacube = '{}' AND tileid = '{}' ".format(params[:-1],scene['sceneid'],scene['datacube'],scene['tileid'])
        do_command(sql)
        # warped file will be _mask.tif instead of _quality.tif
        sql = "UPDATE scenes SET warped = '{}' WHERE id = {}".format(maskfile,scene['id'])
        do_command(sql)

    return 0,'Normal execution'

def merge(scene):
    pass


def blend(scene):
    pass


def publish(scene):
    pass