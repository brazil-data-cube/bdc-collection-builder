import mimetypes
from pathlib import Path
from typing import Optional

import numpy
import rasterio
from bdc_catalog.models import Collection, Item, Tile, db
from bdc_collectors.base import BaseCollection
from flask import current_app
from geoalchemy2.shape import from_shape
from numpngw import write_png
from skimage import exposure
from skimage.transform import resize

from ..collections.index_generator import generate_band_indexes
from ..collections.utils import (create_asset_definition, get_or_create_model,
                                 raster_extent, raster_convexhull, generate_cogs)
from ..constants import COG_MIME_TYPE


def guess_mime_type(extension: str) -> Optional[str]:
    mime = mimetypes.guess_type(extension)

    if mime[0] in COG_MIME_TYPE:
        return COG_MIME_TYPE

    return mime[0]


def create_quick_look(file_output, red_file, green_file, blue_file, rows=768, cols=768, no_data=-9999):
    """Generate a Quick Look file (PNG based) from a list of files.

        Note:
            The file order in ``files`` represents the bands Red, Green and Blue, respectively.

        Exceptions:
            RasterIOError when could not open a raster file band

        Args:
            file_output: Path to store the quicklook file.
            red_file: Path to the band attached into red channel.
            green_file: Path to the band attached into green channel.
            blue_file: Path to the band attached into blue channel.
            rows: Image height. Default is 768.
            cols: Image width. Default is 768.
            no_data: Use custom value for nodata.
        """
    image = numpy.zeros((rows, cols, 3,), dtype=numpy.uint8)

    nb = 0
    for band in [red_file, green_file, blue_file]:
        with rasterio.open(band) as data_set:
            raster = data_set.read(1)

        raster = resize(raster, (rows, cols), order=1, preserve_range=True)
        no_data_pos = raster == no_data
        # Evaluate minimum and maximum values
        a = numpy.array(raster.flatten())
        p1, p99 = numpy.percentile(a[a > 0], (1, 99))
        # Convert minimum and maximum values to 1,255 - 0 is nodata
        raster = exposure.rescale_intensity(raster, in_range=(p1, p99), out_range=(1, 255)).astype(numpy.uint8)
        image[:, :, nb] = raster.astype(numpy.uint8) * numpy.invert(no_data_pos)
        nb += 1

    write_png(str(file_output), image, transparent=(0, 0, 0))


def _asset_definition(path, band, is_raster):
    href = _item_prefix(path)

    if band.mime_type:
        mime_type = band.mime_type.name
    else:
        mime_type = guess_mime_type(path.name)

    return create_asset_definition(
        href=href,
        mime_type=mime_type,
        role=['data'],
        absolute_path=str(path),
        is_raster=is_raster
    )


def _item_prefix(path: Path) -> str:
    """Retrieve the bdc_catalog.models.Item prefix used in assets."""
    href = f'/{str(path.relative_to(current_app.config["DATA_DIR"]))}'

    if current_app.config['USE_BUCKET_PREFIX']:
        return href.replace('/Repository/Archive/', current_app.config['AWS_BUCKET_NAME'])

    return href


def get_item_path(relative: str) -> str:
    """Retrieve the Item absolute path from published asset."""
    path = Path(relative.replace('/', '', 1))

    if current_app.config['USE_BUCKET_PREFIX']:
        path = path.relative_to(current_app.config['AWS_BUCKET_NAME']) / 'Repository/Archive'

    data_dir = Path(current_app.config['DATA_DIR'])

    return str(data_dir / path)


def publish_collection(scene_id: str, data: BaseCollection, collection: Collection, cloud_cover=None) -> Item:
    """Generate the Cloud Optimized Files for Image Collection and publish meta information in database.

    Notes:
        This method relies on bdc_collectors.base.BaseCollection definition.

    Raises:
        NotFound When tile information not found in database.
        Exception When could not generate Cloud Optimized File.

    Args:
        scene_id - Scene id reference
        data - Provider collection structure
        collection - Current collection scope

    Returns:
        The created collection item.
    """
    files = data.get_files(collection)

    assets = dict()

    tile = Tile.query().filter(
        Tile.name == data.parser.tile_id(),
        Tile.grid_ref_sys_id == collection.grid_ref_sys_id
    ).first_or_404()

    geom = convex_hull = None

    file_band_map = dict()

    collection_band_map = {b.name: b for b in collection.bands}

    for file in files:
        path = Path(file)
        file = str(file)

        # TODO: Define way to identify raster to support others collection
        is_raster = path.suffix.lower() in ('.tif', '.jp2')

        if is_raster:
            generate_cogs(file, file)

        for band in collection.bands:
            if path.stem.endswith(band.name):
                file_band_map[band.name] = file

                if geom is None or convex_hull is None:
                    geom = from_shape(raster_extent(file), srid=4326)
                    # Trust in band metadata (no data)
                    convex_hull = raster_convexhull(file, no_data=band.nodata)

                    if convex_hull.area > 0.0:
                        convex_hull = from_shape(convex_hull, srid=4326)

                assets[band.name] = _asset_definition(path, band, is_raster)

                break

    index_bands = generate_band_indexes(scene_id, collection, file_band_map)

    for band_name, band_file in index_bands.items():
        path = Path(band_file)

        assets[band_name] = _asset_definition(path, collection_band_map[band_name], is_raster=True)

    if collection.quicklook:
        collection_bands = {b.id: b.name for b in collection.bands}

        red_file = file_band_map[collection_bands[collection.quicklook[0].red]]
        green_file = file_band_map[collection_bands[collection.quicklook[0].green]]
        blue_file = file_band_map[collection_bands[collection.quicklook[0].blue]]

        quicklook = Path(red_file).parent / f'{scene_id}.png'

        create_quick_look(str(quicklook), red_file, green_file, blue_file)

        relative_quicklook = _item_prefix(quicklook)

        assets['thumbnail'] = create_asset_definition(
            href=relative_quicklook,
            mime_type='.png',
            role=['thumbnail'],
            absolute_path=str(quicklook)
        )

    # TODO: Log files/bands which was not published.

    with db.session.begin_nested():
        item_defaults = dict(
            start_date=data.parser.sensing_date(),
            end_date=data.parser.sensing_date()
        )

        where = dict(name=scene_id, collection_id=collection.id)
        item, created = get_or_create_model(Item, defaults=item_defaults, **where)
        item.assets = assets
        item.convex_hull = convex_hull
        item.geom = geom
        item.srid = 4326  # TODO: Add it dynamically
        item.cloud_cover = cloud_cover
        item.tile_id = tile.id
        item.save(commit=False)

    db.session.commit()

    return item
