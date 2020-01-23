# Python Native
from os import makedirs, path as resource_path
from pathlib import Path
from shutil import unpack_archive
import glob
import logging

# 3rdparty
from gdal import GA_ReadOnly, Open as GDALOpen
from numpngw import write_png
from skimage import exposure
from skimage.transform import resize
import numpy

# BDC Scripts
from bdc_db.models import Asset, Band, Collection, CollectionItem, CollectionTile, db
from bdc_scripts.config import Config
from bdc_scripts.db import add_instance, commit, db_aws
from bdc_scripts.core.utils import generate_evi_ndvi, generate_cogs
from bdc_scripts.radcor.forms import CollectionItemForm
from bdc_scripts.radcor.utils import get_or_create_model
from bdc_scripts.radcor.models import RadcorActivity


# Get the product files
BAND_MAP_SR = {
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

BAND_MAP_DN = {
    'coastal': 'B1',
    'blue': 'B2',
    'green': 'B3',
    'red': 'B4',
    'nir': 'B5',
    'swir1': 'B6',
    'swir2': 'B7',
    'quality': 'BQA',
    'panchromatic': 'B8',
    'cirrus': 'B9',
    'tirs1': 'B10',
    'tirs2': 'B11'
}


DEFAULT_QUICK_LOOK_BANDS = ["swir2", "nir", "red"]


def generate_vi(productdir, files):
    fragments = Path(files['red']).stem.split('_')
    pattern = "_".join(fragments[:-1])

    ndvi_name = resource_path.join(productdir, pattern[:-1] + '_ndvi.tif')
    evi_name = resource_path.join(productdir, pattern[:-1] + '_evi.tif')
    files['ndvi'] = ndvi_name
    files['evi'] = evi_name

    generate_evi_ndvi(files['red'], files['nir'], files['blue'], evi_name, ndvi_name)


def uncompress(file_path, destination):
    unpack_archive(file_path, destination)
    return destination


def publish(collection_item: CollectionItem, scene: RadcorActivity):
    identifier = scene.sceneid
    cc = identifier.split('_')
    pathrow = cc[2]
    date = cc[3]
    yyyymm = '{}-{}'.format(date[0:4], date[4:6])

    productdir = scene.args.get('file')

    if productdir and productdir.endswith('.gz'):
        target_dir = Path(Config.DATA_DIR) / 'Repository/Archive/{}/{}/{}'.format(collection_item.collection_id, yyyymm, pathrow)
        makedirs(target_dir, exist_ok=True)

        productdir = uncompress(productdir, str(target_dir))

    collection = Collection.query().filter(Collection.id == collection_item.collection_id).one()
    quicklook = collection.bands_quicklook.split(',') if collection.bands_quicklook else DEFAULT_QUICK_LOOK_BANDS

    files = {}
    qlfiles = {}

    if collection.id == 'LC8DN':
        bands = BAND_MAP_DN
    else:
        bands = BAND_MAP_SR

    for gband, band in bands.items():
        template = productdir+'/LC08_*_{}_{}_*_{}.*'.format(pathrow, date, band)
        fs = glob.glob(template)

        if not fs:
            continue

        if fs[0].lower().endswith('.tif'):
            files[gband] = fs[0]
            if gband in quicklook:
                qlfiles[gband] = fs[0]

    # Skip EVI/NDVI generation for Surface Reflectance
    # since the espa-science already done
    if collection.id == 'LC8DN':
        generate_vi(productdir, files)

    # Cog files
    for band, file_path in files.items():
        # Set destination of COG file
        files[band] = generate_cogs(file_path, file_path)

    # Extract basic scene information and build the quicklook
    pngname = productdir+'/{}.png'.format(identifier)

    dataset = GDALOpen(qlfiles['nir'], GA_ReadOnly)
    numlin = 768
    numcol = int(float(dataset.RasterXSize)/float(dataset.RasterYSize)*numlin)
    image = numpy.zeros((numlin, numcol, len(qlfiles),), dtype=numpy.uint8)

    del dataset

    nb = 0
    for band in quicklook:
        template = qlfiles[band]
        dataset = GDALOpen(template, GA_ReadOnly)
        raster = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)

        del dataset

        raster = resize(raster,(numlin,numcol), order=1, preserve_range=True)
        nodata = raster == -9999
        # Evaluate minimum and maximum values
        a = numpy.array(raster.flatten())
        p1, p99 = numpy.percentile(a[a>0], (1, 99))
        # Convert minimum and maximum values to 1,255 - 0 is nodata
        raster = exposure.rescale_intensity(raster, in_range=(p1, p99),out_range=(1, 255)).astype(numpy.uint8)
        image[:, :, nb] = raster.astype(numpy.uint8) * numpy.invert(nodata)
        nb += 1

    write_png(pngname, image, transparent=(0, 0, 0))

    productdir = productdir.replace(Config.DATA_DIR, '')

    assets_to_upload = {
        'quicklook': dict(
            file=pngname,
            asset=productdir.replace('/Repository/Archive', '')
        )
    }

    for instance in ['local', 'aws']:
        engine_instance = {
            'local': db,
            'aws': db_aws
        }
        engine = engine_instance[instance]

        # Skip catalog on aws for digital number
        if collection_item.collection_id == 'LC8DN' and instance == 'aws':
            continue

        if instance == 'aws':
            asset_url = productdir.replace('/Repository/Archive', Config.AWS_BUCKET_NAME)
        else:
            asset_url = productdir

        pngname = resource_path.join(asset_url, Path(pngname).name)

        assets_to_upload['quicklook']['asset'] = pngname

        with engine.session.begin_nested():
            with engine.session.no_autoflush:
                # Add collection item to the session if not present
                if collection_item not in engine.session:
                    item = engine.session.query(CollectionItem).filter(CollectionItem.id == collection_item.id).first()

                    if not item:
                        cloned_properties = CollectionItemForm().dump(collection_item)
                        collection_item = CollectionItem(**cloned_properties)
                        engine.session.add(collection_item)

                collection_item.quicklook = pngname

                restriction = dict(
                    grs_schema_id=collection_item.grs_schema_id,
                    tile_id=collection_item.tile_id,
                    collection_id=collection_item.collection_id
                )

                _, _ = get_or_create_model(CollectionTile, defaults=restriction, engine=engine, **restriction)

                collection_bands = engine.session.query(Band).filter(Band.collection_id == collection_item.collection_id).all()

                # Inserting data into Product table
                for band in files:
                    template = resource_path.join(asset_url, Path(files[band]).name)

                    dataset = GDALOpen(files[band], GA_ReadOnly)
                    asset_band = dataset.GetRasterBand(1)

                    chunk_x, chunk_y = asset_band.GetBlockSize()

                    band_model = next(filter(lambda b: band == b.common_name, collection_bands), None)

                    if not band_model:
                        logging.warning('Band {} of collection {} not found in database. Skipping...'.format(
                            band, collection_item.collection_id))
                        continue

                    defaults = dict(
                        url=template,
                        source=cc[0],
                        raster_size_x=dataset.RasterXSize,
                        raster_size_y=dataset.RasterYSize,
                        raster_size_t=1,
                        chunk_size_t=1,
                        chunk_size_x=chunk_x,
                        chunk_size_y=chunk_y
                    )

                    asset, _ = get_or_create_model(
                        Asset,
                        engine=engine,
                        defaults=defaults,
                        collection_id=scene.collection_id,
                        band_id=band_model.id,
                        grs_schema_id=scene.collection.grs_schema_id,
                        tile_id=collection_item.tile_id,
                        collection_item_id=collection_item.id,
                    )

                    assets_to_upload[band] = dict(file=files[band], asset=asset.url)

                    # Add into scope of local and remote database
                    add_instance(engine, asset)

            # Persist database
        commit(engine)

    return assets_to_upload
