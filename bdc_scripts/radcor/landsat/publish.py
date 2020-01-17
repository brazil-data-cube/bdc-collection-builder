# Python Native
import glob
import logging

# 3rdparty
from gdal import GA_ReadOnly, Open as GDALOpen
from numpngw import write_png
from skimage import exposure
from skimage.transform import resize
import numpy

# BDC Scripts
from bdc_db.models import Asset, Band, CollectionItem, CollectionTile, db
from bdc_scripts.db import add_instance, commit
from bdc_scripts.radcor.utils import get_or_create_model
from bdc_scripts.radcor.models import RadcorActivity


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

def publish(collection_item: CollectionItem, scene: RadcorActivity):
    identifier = scene.sceneid
    cc = identifier.split('_')
    pathrow = cc[2]
    date = cc[3]

    productdir = scene.args.get('file')

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

    with db.session.begin_nested():
        collection_item.quicklook = pngname

        restriction = dict(
            grs_schema_id=collection_item.grs_schema_id,
            tile_id=collection_item.tile_id,
            collection_id=collection_item.collection_id
        )

        collection_tile, _ = get_or_create_model(CollectionTile, defaults=restriction, **restriction)

        # Add into scope of local and remote database
        add_instance(collection_item, collection_tile)

        collection_bands = Band.query().filter(Band.collection_id == collection_item.collection_id).all()

        # Inserting data into Product table
        for band in bandmap:
            template = files[band]

            dataset = GDALOpen(template, GA_ReadOnly)
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
                defaults=defaults,
                collection_id=scene.collection_id,
                band_id=band_model.id,
                grs_schema_id=scene.collection.grs_schema_id,
                tile_id=collection_item.tile_id,
                collection_item_id=collection_item.id,
            )

            # Add into scope of local and remote database
            add_instance(asset)

    # Persist database
    commit()

    return 0
