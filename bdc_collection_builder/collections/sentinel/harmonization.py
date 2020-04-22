# Python Native
import glob
import logging
import os
import re
import shutil
import xml.etree.ElementTree as ET

# 3rd-party
from osgeo import gdal, osr, ogr
from pathlib import Path
import numpy

# BDC Scripts
from ...config import Config
from ..nbar import process_NBAR
from ..utils import load_img, generate_cogs
from .utils import get_jp2_files, load_img_resampled_to_half
################################################################################
## Generate Sentinel Angle view bands
################################################################################

def get_tileid(XML_File):
    """Get tileid using .SAFE XML_File."""
    tile_id = ""
    # Parse the XML file 
    tree = ET.parse(XML_File)
    root = tree.getroot()

    # Find the angles
    for child in root:
        if child.tag[-12:] == 'General_Info':
            geninfo = child

    for segment in geninfo:
        if segment.tag == 'TILE_ID':
            tile_id = segment.text.strip()
    return(tile_id)


def get_sun_angles(XML_File):
    """Get sun azimuth and sun zenith angles using .SAFE XML_File."""
    solar_zenith_values = numpy.empty((23,23,)) * numpy.nan #initiates matrix
    solar_azimuth_values = numpy.empty((23,23,)) * numpy.nan

    # Parse the XML file 
    tree = ET.parse(XML_File)
    root = tree.getroot()

    # Find the angles
    for child in root:
        if child.tag[-14:] == 'Geometric_Info':
            geoinfo = child

    for segment in geoinfo:
        if segment.tag == 'Tile_Angles':
            angles = segment

    for angle in angles:
        if angle.tag == 'Sun_Angles_Grid':
            for bset in angle:
                if bset.tag == 'Zenith':
                    zenith = bset
                if bset.tag == 'Azimuth':
                    azimuth = bset
            for field in zenith:
                if field.tag == 'Values_List':
                    zvallist = field
            for field in azimuth:
                if field.tag == 'Values_List':
                    avallist = field
            for rindex in range(len(zvallist)):
                zvalrow = zvallist[rindex]
                avalrow = avallist[rindex]
                zvalues = zvalrow.text.split(' ')
                avalues = avalrow.text.split(' ')
                values = list(zip( zvalues, avalues )) #row of values
                for cindex in range(len(values)):
                    if ( values[cindex][0] != 'NaN' and values[cindex][1] != 'NaN' ):
                        zen = float(values[cindex][0] )
                        az = float(values[cindex][1] )
                        solar_zenith_values[rindex,cindex] = zen
                        solar_azimuth_values[rindex,cindex] = az
    return (solar_zenith_values, solar_azimuth_values)


def get_sensor_angles(XML_File):
    """Get sensor azimuth and sun zenith angles using .SAFE XML_File."""
    numband = 13
    sensor_zenith_values = numpy.empty((numband,23,23)) * numpy.nan #initiates matrix
    sensor_azimuth_values = numpy.empty((numband,23,23)) * numpy.nan

    # Parse the XML file 
    tree = ET.parse(XML_File)
    root = tree.getroot()

    # Find the angles
    for child in root:
        if child.tag[-14:] == 'Geometric_Info':
            geoinfo = child

    for segment in geoinfo:
        if segment.tag == 'Tile_Angles':
            angles = segment

    for angle in angles:
        if angle.tag == 'Viewing_Incidence_Angles_Grids':
            bandId = int(angle.attrib['bandId'])
            for bset in angle:
                if bset.tag == 'Zenith':
                    zenith = bset
                if bset.tag == 'Azimuth':
                    azimuth = bset
            for field in zenith:
                if field.tag == 'Values_List':
                    zvallist = field
            for field in azimuth:
                if field.tag == 'Values_List':
                    avallist = field
            for rindex in range(len(zvallist)):
                zvalrow = zvallist[rindex]
                avalrow = avallist[rindex]
                zvalues = zvalrow.text.split(' ')
                avalues = avalrow.text.split(' ')
                values = list(zip( zvalues, avalues )) #row of values
                for cindex in range(len(values)):
                    if ( values[cindex][0] != 'NaN' and values[cindex][1] != 'NaN' ):
                        zen = float( values[cindex][0] )
                        az = float( values[cindex][1] )
                        sensor_zenith_values[bandId, rindex,cindex] = zen
                        sensor_azimuth_values[bandId, rindex,cindex] = az
    return(sensor_zenith_values,sensor_azimuth_values)


def write_intermediary(newRasterfn,rasterOrigin,proj, array):
    """Write intermediary (angle) files."""
    cols = array.shape[1]
    rows = array.shape[0]
    originX = rasterOrigin[0]
    originY = rasterOrigin[1]

    driver = gdal.GetDriverByName('GTiff')
    outRaster = driver.Create(newRasterfn, cols, rows, 1, gdal.GDT_Float32)
    outRaster.SetGeoTransform((originX, 5000, 0, originY, 0, -5000))
    outband = outRaster.GetRasterBand(1)
    outband.WriteArray(array)
    outRaster.SetProjection( proj )
    outband.FlushCache()


def generate_anglebands(XMLfile):
    """Generate Sentinel-2 Angle Bands inside .SAFE."""
    path = os.path.split(XMLfile)[0]
    imgFolder = path + "/IMG_DATA/"
    angFolder = path + "/ANG_DATA/"
    os.makedirs(angFolder, exist_ok=True)

    #use band 4 as reference due to 10m spatial resolution
    imgref = [f for f in glob.glob(imgFolder + "**/*04.jp2", recursive=True)][0]

    tmp_ds = gdal.Open(imgref)
    tmp_ds.GetRasterBand(1).SetNoDataValue(numpy.nan)
    geotrans = tmp_ds.GetGeoTransform()  #get GeoTranform from existed 'data0'
    proj = tmp_ds.GetProjection() #you can get from a exsited tif or import 

    scenename = get_tileid(XMLfile)
    solar_zenith, solar_azimuth = get_sun_angles(XMLfile)
    sensor_zenith, sensor_azimuth = get_sensor_angles(XMLfile)

    rasterOrigin = (geotrans[0],geotrans[3])

    write_intermediary((angFolder + scenename + "solar_zenith"),rasterOrigin,proj,solar_zenith)
    write_intermediary((angFolder + scenename + "solar_azimuth"),rasterOrigin,proj,solar_azimuth)
    for num_band in (range(len(sensor_azimuth))):
        write_intermediary((angFolder + scenename + "sensor_zenith_b" + str(num_band)),rasterOrigin,proj,sensor_zenith[num_band])
        write_intermediary((angFolder + scenename + "sensor_azimuth_b" + str(num_band)),rasterOrigin,proj,sensor_azimuth[num_band])

    del tmp_ds


def resample_anglebands(ang_matrix, imgref, filename):
    """Resample Sentinel-2 Angle Bands."""
    src_ds = gdal.Open(imgref)
    src_ds.GetRasterBand(1).SetNoDataValue(numpy.nan)
    geotrans = src_ds.GetGeoTransform()  #get GeoTranform from existed 'data0'
    proj = src_ds.GetProjection() #you can get from a exsited tif or import 

    cols = src_ds.RasterXSize
    rows = src_ds.RasterYSize

    rasterOrigin = (geotrans[0],geotrans[3])

    # Now, we create an in-memory raster
    mem_drv = gdal.GetDriverByName('MEM')
    tmp_ds = mem_drv.Create('', len(ang_matrix[0]), len(ang_matrix), 1, gdal.GDT_Float32)

    # Set the geotransform
    tmp_ds.SetGeoTransform((rasterOrigin[0], 5000, 0, rasterOrigin[1], 0, -5000))
    tmp_ds.SetProjection (proj)
    tmp_ds.GetRasterBand(1).SetNoDataValue(numpy.nan)
    tmp_ds.GetRasterBand(1).WriteArray(ang_matrix)

    driver = gdal.GetDriverByName('GTiff')
    dst_ds = driver.Create(filename, cols, rows, 1, gdal.GDT_Float32)
    dst_ds.SetGeoTransform(geotrans)
    dst_ds.SetProjection(proj)

    resampling = gdal.GRA_Bilinear
    gdal.ReprojectImage(tmp_ds, dst_ds, tmp_ds.GetProjection(), dst_ds.GetProjection(), resampling)

    del src_ds
    del tmp_ds
    del dst_ds


def generate_resampled_anglebands(XMLfile):
    """Generate Resampled Sentinel-2 Angle Bands inside .SAFE."""
    path = os.path.split(XMLfile)[0]
    imgFolder = path + "/IMG_DATA/"
    angFolder = path + "/ANG_DATA/"
    os.makedirs(angFolder, exist_ok=True)

    #use band 4 as reference due to 10m spatial resolution
    imgref = [f for f in glob.glob(imgFolder + "**/*04.jp2", recursive=True)][0]

    scenename = get_tileid(XMLfile)
    solar_zenith, solar_azimuth = get_sun_angles(XMLfile)
    sensor_zenith, sensor_azimuth = get_sensor_angles(XMLfile)

    sensor_zenith_mean = sensor_zenith[0]
    sensor_azimuth_mean = sensor_azimuth[0]
    for num_band in (range(1,len(sensor_azimuth))):
        sensor_zenith_mean += sensor_zenith[num_band]
        sensor_azimuth_mean += sensor_azimuth[num_band]
    sensor_zenith_mean /= len(sensor_azimuth)
    sensor_azimuth_mean /= len(sensor_azimuth)

    sz_path = angFolder + scenename + '_solar_zenith_resampled.tif'
    sa_path = angFolder + scenename + '_solar_azimuth_resampled.tif'
    vz_path = angFolder + scenename + '_sensor_zenith_mean_resampled.tif'
    va_path = angFolder + scenename + '_sensor_azimuth_mean_resampled.tif'

    resample_anglebands(solar_zenith, imgref, sz_path)
    resample_anglebands(solar_azimuth, imgref, sa_path)
    resample_anglebands(sensor_zenith_mean, imgref, vz_path)
    resample_anglebands(sensor_azimuth_mean, imgref, va_path)

    return sz_path, sa_path, vz_path, va_path


def xml_from_safe(SAFEfile):
    """Obtain MTD_TL.xml file from a .SAFE."""
    return os.path.join(SAFEfile, 'GRANULE', os.path.join(os.listdir(os.path.join(SAFEfile,'GRANULE/'))[0], 'MTD_TL.xml'))


def gen_s2_ang(SAFEfile):
    """Generate Sentinel-2 Angle bands"""
    xml = xml_from_safe(SAFEfile)
    ### generate 23x23 Product (not resampled)
    # generate_anglebands(os.path.join(SAFEfile, 'GRANULE', os.path.join(os.listdir(os.path.join(SAFEfile,'GRANULE/'))[0], 'MTD_TL.xml')))

    ### Generates resampled anglebands (to 10m)
    sz_path, sa_path, vz_path, va_path = generate_resampled_anglebands(xml)
    return sz_path, sa_path, vz_path, va_path


def sentinel_NBAR(sz_path, sa_path, vz_path, va_path, dir_published_L2, target_dir):
    """Prepare Sentinel-2 angle bands and process NBAR."""
    ### Sentinel-2 data set ###
    pars_array_index = {'B02': 0, 'B03': 1, 'B04': 2, 'B08': 3, 'B8A': 3, 'B11': 4, 'B12': 5}

    satsen = os.path.basename(dir_published_L2)[0:3]
    logging.info('SatSen: {}'.format(satsen))

    img_dir = dir_published_L2
    bands10m = ['B02','B03','B04', 'B08']
    band_sz = load_img(sz_path)
    band_sa = load_img(sa_path)
    band_vz = load_img(vz_path)
    band_va = load_img(va_path)
    process_NBAR(img_dir, bands10m, band_sz, band_sa, band_vz, band_va, satsen, pars_array_index, target_dir)

    bands20m = ['B8A','B11','B12']
    band_sz = load_img_resampled_to_half(sz_path)
    band_sa = load_img_resampled_to_half(sa_path)
    band_vz = load_img_resampled_to_half(vz_path)
    band_va = load_img_resampled_to_half(va_path)
    process_NBAR(img_dir, bands20m, band_sz, band_sa, band_vz, band_va, satsen, pars_array_index, target_dir)


def sentinel_harmonize(SAFEL1C, dir_published_L2, target_dir=None):
    logging.info('Generating Angles from {} ...'.format(SAFEL1C))
    sz_path, sa_path, vz_path, va_path = gen_s2_ang(SAFEL1C)

    ### if target_dir is not given create HARMONIZED_DATA folder inside GRANULE folder
    if target_dir is None:
        target_dir = os.path.join(dir_published_L2, '/HARMONIZED_DATA/')
    os.makedirs(target_dir, exist_ok=True)

    logging.info('Harmonization ...')
    sentinel_NBAR(sz_path, sa_path, vz_path, va_path, dir_published_L2, target_dir)

    #COPY quality band and TCI
    pattern = re.compile('.*SCL.*')
    img_list = [f for f in glob.glob(dir_published_L2 + "/*.tif", recursive=True)]
    qa_filepath = list(filter(pattern.match, img_list))[0]
    shutil.copy(qa_filepath, target_dir)
    img_list = [f for f in glob.glob(dir_published_L2 + "/*.png", recursive=True)]
    png_filepath = img_list[0]
    shutil.copy(png_filepath, target_dir)

    return target_dir
