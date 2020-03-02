# Ross-thick Li-sparse model in:
# Lucht, W., Schaaf, C. B., & Strahler, A. H. (2000). 
# An algorithm for the retrieval of albedo from space using semiempirical BRDF models. 
# IEEE Transactions on Geoscience and Remote Sensing, 38(2), 977-998.

# Python Native
import logging
import os
import re

# 3rd-party
import numpy
import rasterio


# Coeffients in  Roy, D. P., Zhang, H. K., Ju, J., Gomez-Dans, J. L., Lewis, P. E., Schaaf, C. B., Sun Q., Li J., Huang H., & Kovalskyy, V. (2016). 
# A general method to normalize Landsat reflectance data to nadir BRDF adjusted reflectance. 
# Remote Sensing of Environment, 176, 255-271.
pars_array = numpy.matrix('774 372 79; 1306 580 178; 1690 574 227; 3093 1535 330; 3430 1154 453; 2658 639 387')

brratio = 1.0
hbratio = 2.0
DE2RA = 0.0174532925199432956

def GetPhaang(cos1, cos2, sin1, sin2, cos3):
    """Calculate Pha Angle."""
    cosres = cos1 * cos2 + sin1 * sin2 * cos3
    res = numpy.arccos(numpy.maximum(-1., numpy.minimum(1., cosres)))
    sinres = numpy.sin(res)

    return {"cosres": cosres, "res": res, "sinres": sinres}


def GetDistance(tan1, tan2, cos3):
    """Calculate Distance D."""
    temp = tan1 * tan1 + tan2 * tan2 - 2. * tan1 * tan2 * cos3
    res  = numpy.sqrt(numpy.maximum(0., temp))

    return res


def GetpAngles(brratio, tan1):
    """Calculate sinp, cosp and tanp."""
    tanp = brratio * tan1
    tanp[tanp < 0] = 0
    angp = numpy.arctan(tanp)
    sinp = numpy.sin(angp)
    cosp = numpy.cos(angp)

    return {"sinp": sinp, "cosp": cosp, "tanp": tanp}


def GetOverlap(hbratio, distance, cos1, cos2, tan1, tan2, sin3):
    """Calculate Overlap O."""
    temp = 1. / cos1 + 1. / cos2
    cost = hbratio * numpy.sqrt(distance * distance + tan1 * tan1 * tan2 * tan2 * sin3 * sin3) / temp
    cost = numpy.maximum(-1., numpy.minimum(1., cost))
    tvar = numpy.arccos(cost)
    sint = numpy.sin(tvar)
    overlap = 1. / numpy.pi * (tvar - sint * cost) * (temp)
    overlap = numpy.maximum(0., overlap)

    return {"overlap": overlap, "temp": temp}


def LiKernel(hbratio, brratio, tantv, tanti, sinphi, cosphi, SparseFlag, RecipFlag):
    """Calculate Li Sparse geometric kernel."""
    GetpAnglesv = GetpAngles(brratio, tantv)
    GetpAnglesi = GetpAngles(brratio, tanti)
    phaang = GetPhaang(GetpAnglesv['cosp'], GetpAnglesi['cosp'], GetpAnglesv['sinp'], GetpAnglesi['sinp'], cosphi)
    distancep = GetDistance(GetpAnglesv['tanp'], GetpAnglesi['tanp'], cosphi)
    overlap = GetOverlap(hbratio, distancep, GetpAnglesv['cosp'], GetpAnglesi['cosp'], GetpAnglesv['tanp'], GetpAnglesi['tanp'], sinphi)
    if (SparseFlag):
        if (RecipFlag):
            result = (overlap['overlap'] - overlap['temp']) + 1. / 2. * (1. + phaang['cosres']) / GetpAnglesv['cosp'] / GetpAnglesi['cosp']
        else:
            result = overlap['overlap'] - overlap['temp'] + 1. / 2. * (1. + phaang['cosres']) / GetpAnglesv['cosp']
    else:
        if (RecipFlag):
            result = (1 + phaang['cosres']) / (GetpAnglesv['cosp'] * GetpAnglesi['cosp'] * (overlap['temp'] - overlap['overlap'])) - 2.
        else:
            result = (1 + phaang['cosres']) / (GetpAnglesv['cosp'] * (overlap['temp'] - overlap['overlap'])) - 2.

    return result


def CalculateKernels(tv, ti, phi):
    """Calculate volumetric, geometric and isometric kernel."""
    resultsArray = numpy.empty([len(tv), 3])
    resultsArray[:] = numpy.nan

    resultsArray[:, 0] = 1.

    cosphi = numpy.cos(phi)

    costv = numpy.cos(tv)
    costi = numpy.cos(ti)
    sintv = numpy.sin(tv)
    sinti = numpy.sin(ti)
    phaang = GetPhaang(costv, costi, sintv, sinti, cosphi)
    rosselement = (numpy.pi / 2. - phaang['res']) * phaang['cosres'] + phaang['sinres']
    resultsArray[:, 1] = rosselement / (costi + costv) - numpy.pi / 4.

    # /*finish rossthick kernal */
    sinphi = numpy.sin(phi)
    tantv = numpy.tan(tv)
    tanti = numpy.tan(ti)

    SparseFlag = 1
    RecipFlag = 1
    resultsArray[:, 2] = LiKernel(hbratio, brratio, tantv, tanti, sinphi, cosphi, SparseFlag, RecipFlag)

    return resultsArray


def bandpassHLS_1_4(img, band, satsen):
    """Apply sensor/band bandpass value assuming Landsat-8/OLI as satellite/sensor bands as reference.
    
    Values extracted from Skakun et. al (2018).
    """
    logging.debug('Applying bandpass band {} satsen {}'.format(band, satsen))
    #Skakun2018 coefficients
    if (satsen == 'S2A'):
        if (band == 'B01'): #ultraBlue/coastal #MODIS don't have this band
            slope = 0.9959
            offset = -0.0002
        elif (band == 'B02'): #Blue
            slope = 0.9778
            offset = -0.004
        elif (band == 'B03'): #Green
            slope = 1.0053
            offset = -0.0009
        elif (band == 'B04'): #Red
            slope = 0.9765
            offset = 0.0009
        elif (band == 'B8A'): # Narrow Nir
            slope = 0.9983
            offset = -0.0001
        elif (band == 'B11'): #Swir 1
            slope = 0.9987
            offset = -0.0011
        elif (band == 'B12'): #Swir 2
            slope = 1.003
            offset = -0.0012
        img = (img * slope) + offset

    elif (satsen == 'S2B'):
        if (band == 'B01'): #ultraBlue/coastal #MODIS don't have this band
            slope = 0.9959
            offset = -0.0002
        elif (band == 'B02'): #Blue
            slope = 0.9778
            offset = -0.004
        elif (band == 'B03'): #Green
            slope = 1.0075
            offset = -0.0008
        elif (band == 'B04'): #Red
            slope = 0.9761
            offset = 0.001
        elif (band == 'B8A'): # Narrow Nir
            slope = 0.9966
            offset = 0.000
        elif (band == 'B11'): #Swir 1
            slope = 1.000
            offset = -0.0003
        elif (band == 'B12'): #Swir 2
            slope = 0.9867
            offset = -0.0004
        img = (img * slope) + offset

    return img


def calc_kernels(vzn, szn, raa):
    """Calculate kernels converting to radian."""
    nbarkerval = CalculateKernels(vzn*DE2RA, szn*DE2RA, raa*DE2RA)
    return nbarkerval


def calculate_global_kernels(band_sz, band_sa, band_vz, band_va):
    """Calculate common (between all bands) kernels."""
    ### Applying scale factor on angle bands
    solar_zenith = numpy.divide(band_sz, 100)
    view_zenith = numpy.divide(band_vz, 100)
    relative_azimuth = numpy.divide(numpy.subtract(band_va, band_sa), 100)
    solar_zenith_output = numpy.copy(solar_zenith)
    kernel = calc_kernels(view_zenith, solar_zenith, relative_azimuth)
    refkernel = calc_kernels(numpy.zeros(len(view_zenith)), solar_zenith_output, numpy.zeros(len(view_zenith)))

    return kernel, refkernel


def mult_par_kernel(pars, nbarkerval):
    """Perform Kernel Matrix multiplication."""
    ref = nbarkerval.dot(pars)
    return ref


def NBAR_calculate_global_perband(band, kernel, refkernel, b):
    """Calculate NBAR for specific band."""
    sensor_input = band
    sensor_output = band
    notnan_index = ~numpy.isnan(band)
    if (numpy.any(notnan_index)):
        srf1 = mult_par_kernel(pars_array[b,:].T, kernel)
        srf0 = mult_par_kernel(pars_array[b,:].T, refkernel)
        ratio = numpy.ravel(numpy.divide(srf0, srf1).T)
        sensor_output = numpy.multiply(ratio, sensor_input).astype(numpy.int16)

    return sensor_output


def process_NBAR(img_dir, bands, band_sz, band_sa, band_vz, band_va, satsen, pars_array_index, out_dir):
    """Calculate NBAR."""
    imgs = os.listdir(img_dir)

    kernel, refkernel = calculate_global_kernels(band_sz, band_sa, band_vz, band_va)

    for b in bands:
        logging.debug('Harmonization band {}'.format(b))
        r = re.compile('.*_{}.*tif$'.format(b))
        input_file = list(filter(r.match, imgs))[0]
        output_file = out_dir + '/' + input_file[0:-4] + '_NBAR.tif'

        logging.debug('Reading input data ...')
        with rasterio.open(img_dir + '/' + input_file) as dataset:
            band = dataset.read(1)
            nodata = dataset.nodata
            mask = band == nodata
            kwargs = dataset.meta
        band_one = band.flatten()

        logging.debug("Producing NBAR band {} ({}) ...".format(b, pars_array_index[b]))
        band_one = NBAR_calculate_global_perband(band_one, kernel, refkernel, pars_array_index[b])

        if (satsen == 'S2A') or (satsen == 'S2B'):
            band_one = bandpassHLS_1_4(band_one, b, satsen)

        dims = band.shape
        band = band_one.astype(numpy.int16).reshape((dims[0], dims[1]))
        if mask.any():
            band[mask] = nodata

        kwargs['dtype'] = numpy.int16
        kwargs['driver'] = 'Gtiff'
        kwargs['compress'] = 'LZW'
        with rasterio.open(str(output_file), 'w', **kwargs) as dst:
            dst.write_band(1, band)
