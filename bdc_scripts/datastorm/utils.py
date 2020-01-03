from os import path as resource_path
from bdc_db.models import Band
from bdc_scripts.config import Config


def warp(link: str, band: Band):
    scenesrs = osr.SpatialReference()
    scenesrs.ImportFromProj4(scene['srs'])

    # Get the input and warped files name
    filename = '/vsicurl/' + link
    warped = resource_path.join(Config.DATA_DIR, 'Repository/collections/warped/{}.tif')

    # If warped file not exists, reproject the input scene
    if not os.path.exists(warped):
        src_ds = gdal.Open(filename)

        if src_ds is None:
            raise IOError('Dataset not found "{}".'.format(filename))

        src_ds.GetRasterBand(1).SetNoDataValue(0)

        # Now, we create an in-memory raster
        mem_drv = gdal.GetDriverByName('MEM')
        tmp_ds = mem_drv.Create('', scene['numcol'], scene['numlin'], 1, gdal.GDT_UInt16)

        # Set the geotransform
        tmp_ds.SetGeoTransform([scene['xmin'], scene['resx'], 0, scene['ymax'], 0, -scene['resy']])
        tmp_ds.SetProjection ( scenesrs.ExportToWkt() )
        tmp_ds.GetRasterBand(1).SetNoDataValue(0)

        # Perform the projection/resampling
        if band.common_name == 'quality':
            resampling = gdal.GRA_NearestNeighbour
        else:
            resampling = gdal.GRA_Bilinear
        error_threshold = 0.125
        try:
            res = gdal.ReprojectImage(src_ds, tmp_ds, src_ds.GetProjection(), tmp_ds.GetProjection(), resampling)
        except:
            # Update all bands with enable status
            sql = "UPDATE scenes SET enabled = 0 WHERE sceneid = '{}' AND datacube = '{}' AND tileid = '{}'".format(scene['sceneid'],scene['datacube'],scene['tileid'])
            do_command(sql)
            return 1,'Error on ReprojectImage'

        # Create the final warped raster
        driver = gdal.GetDriverByName('GTiff')
        dst_ds = driver.CreateCopy(warped, tmp_ds,  options = [ 'COMPRESS=LZW', 'TILED=YES' ] )
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