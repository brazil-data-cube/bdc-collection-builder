#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2022 INPE.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/gpl-3.0.html>.
#

"""Module to publish an collection item on database."""
import datetime
import logging
import mimetypes
import os
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional
from xml.etree import ElementTree

import numpy
import rasterio
import shapely.geometry
from bdc_catalog.models import Collection, Item, Provider, Tile, db
from bdc_collectors.base import BaseCollection
from flask import current_app
from geoalchemy2.shape import from_shape
from numpngw import write_png
from PIL import Image

from ..collections.index_generator import generate_band_indexes
from ..collections.utils import (generate_cogs, get_epsg_srid, get_or_create_model,
                                 is_sen2cor, raster_convexhull, raster_extent)
from ..config import Config
from ..constants import COG_MIME_TYPE, DEFAULT_SRID
import re


def guess_mime_type(extension: str, cog=False) -> Optional[str]:
    """Try to identify file mimetype."""
    mime = mimetypes.guess_type(extension)

    if mime[0] in COG_MIME_TYPE and cog:
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
            raster = data_set.read(1, out_shape=(rows, cols))

        nodata_values = raster == no_data
        if raster.min() != 0 or raster.max() != 0:
            raster = raster.astype(numpy.float32) / 10000. * 255.
            raster[raster > 255] = 255
        image[:, :, nb] = raster.astype(numpy.uint8) * numpy.invert(nodata_values)
        nb += 1

    write_png(str(file_output), image, transparent=(0, 0, 0))


def compress_raster(input_path: str, output_path: str, algorithm: str = 'deflate'):
    """Compress a raster using GDAL compression algorithm."""
    with TemporaryDirectory() as tmp:
        tmp_file = Path(tmp) / Path(input_path).name

        with rasterio.open(str(input_path)) as dataset:
            profile = dataset.profile.copy()

            profile.update(
                compress=algorithm
            )

            with rasterio.open(str(tmp_file), 'w', **profile) as ds:
                for band_idx in range(1, dataset.count + 1):
                    array = dataset.read(band_idx)
                    ds.write(array, band_idx)

        shutil.move(tmp_file, output_path)


def _asset_definition(path, band=None, is_raster=False, cog=False, role=['data'], **options):
    href = _item_prefix(path, **options)

    if band and band.mime_type:
        mime_type = band.mime_type.name
    else:
        mime_type = guess_mime_type(path.name, cog=cog)

    return Item.create_asset_definition(
        file=str(path),
        href=href,
        mime_type=mime_type,
        role=role,
        is_raster=is_raster
    )


def _item_prefix(path: Path, prefix=None, item_prefix=None) -> str:
    """Retrieve the bdc_catalog.models.Item prefix used in assets."""
    if prefix is None:
        prefix = current_app.config["DATA_DIR"]

    href = path.relative_to(prefix)

    if current_app.config['USE_BUCKET_PREFIX']:
        return str(Path(current_app.config['AWS_BUCKET_NAME']) / href)

    if item_prefix:
        href = Path(item_prefix) / href

    return str(href)


def get_item_path(relative: str) -> str:
    """Retrieve the Item absolute path from published asset."""
    path = Path(relative.replace('/', '', 1))

    if current_app.config['USE_BUCKET_PREFIX']:
        path = path.relative_to(current_app.config['AWS_BUCKET_NAME']) / 'Repository/Archive'

    data_dir = Path(current_app.config['DATA_DIR'])

    return str(data_dir / path)


def get_footprint_sentinel(mtd_file: str) -> shapely.geometry.Polygon:
    """Get image footprint from a Sentinel-2 MTD file."""
    tree = ElementTree.parse(str(mtd_file))
    footprint = None

    for element in tree.findall('.//EXT_POS_LIST'):
        footprint = element.text.rstrip()
        break

    footprint_array = footprint.split(' ')

    points = [(float(footprint_array[i + 1]), float(footprint_array[i])) for i in range(0, len(footprint_array), 2)]

    footprint_linear_ring = shapely.geometry.LinearRing(points)

    return shapely.geometry.Polygon(footprint_linear_ring)


def generate_quicklook_pvi(safe_folder: Path, quicklook: Path):
    """Generate QuickLook preview from a Sentinel-2 PVI file."""
    pvi = list(safe_folder.rglob('**/*PVI*.jp2'))[0]

    Image.open(str(pvi)).save(str(quicklook))


def publish_collection_item(scene_id: str, data: BaseCollection, collection: Collection, file: str,
                       cloud_cover=None, provider_id: Optional[int] = None, scene_meta=None, **kwargs) -> Item:
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
        file - Path to seek

    Returns:
        The created collection item.
    """
    file_band_map = dict()
    assets = dict()
    old_file_path = file
    asset_item_prefix = Config.ITEM_PREFIX
    prefix = Config.PUBLISH_DATA_DIR
    path_include_month = kwargs['activity'].get('path_include_month')

    temporary_dir = TemporaryDirectory()
    srid = DEFAULT_SRID

    data_prefix = Config.PUBLISH_DATA_DIR
    if collection.collection_type == 'cube':
        data_prefix = Config.CUBES_DATA_DIR
        if not data_prefix.endswith('/composed'):
            data_prefix = os.path.join(data_prefix, 'composed')

    # Special treatment for file partially processed
    if not os.path.exists(file):
        item: Optional[Item] = Item.query().filter(Item.name == scene_id, Item.collection_id == collection.id).first()
        if item is None:
            raise IOError(f"File {file} not found.")
        # TODO: validate the assets paths
        logging.info(f"Item {item.name} published")
        return item

    # Get Destination Folder
    destination = data.path(collection, prefix=data_prefix, path_include_month=path_include_month)

    is_sen2cor_flag = is_sen2cor(collection)

    geom = convex_hull = None

    is_compressed = str(file).endswith('.zip') or str(file).endswith('.tar.gz')
    quicklook = None

    if is_compressed:
        destination = data.compressed_file(collection, path_include_month=path_include_month).parent

        tmp = Path(temporary_dir.name)

        file_path = Path(file)

        destination_file = destination / file_path.name

        file = file if file_path.exists() else destination_file

        shutil.unpack_archive(
            file,
            temporary_dir.name
        )

        quicklook = Path(destination) / f'{scene_id}.png'

        assets['asset'] = Item.create_asset_definition(
            href=_item_prefix(Path(file), item_prefix=asset_item_prefix),
            mime_type=guess_mime_type(file),
            role=['data'],
            file=str(file)
        )

        if scene_id.startswith('S2'):
            pvi = list(tmp.rglob('**/*PVI*.jp2'))[0]
            band2 = list(tmp.rglob('**/*B02.jp2'))[0]
            srid = get_epsg_srid(str(band2))

            mtd = '**/MTD_MSIL1C.xml'
            if '_MSIL2A_' in scene_id:
                mtd = '**/MTD_MSIL2A.xml'
            mtd = list(tmp.rglob(mtd))[0]

            geom = from_shape(raster_extent(str(band2)), srid=4326)
            convex_hull = from_shape(get_footprint_sentinel(str(mtd)), srid=4326)

            quicklook.parent.mkdir(exist_ok=True, parents=True)
            Image.open(str(pvi)).save(str(quicklook))

            assets['thumbnail'] = Item.create_asset_definition(
                href=_item_prefix(quicklook, item_prefix=asset_item_prefix),
                mime_type=guess_mime_type(str(quicklook)),
                role=['thumbnail'],
                file=str(quicklook)
            )
        elif data.parser.source() in ('LC09', 'LC08', 'LE07', 'LT05', 'LT04'):
            file_band_map = data.get_files(collection, path=tmp)
            band_ref = 'B2' if int(data.parser.level()) == 1 else 'SR_B2'
            band2 = str(file_band_map[band_ref])
            srid = get_epsg_srid(str(band2))
            geom = from_shape(raster_extent(band2), srid=4326)
            convex_hull = from_shape(raster_convexhull(band2, no_data=0), srid=4326)
            file = Path(file).parent
    else:
        destination.mkdir(parents=True, exist_ok=True)

    tile_id = data.parser.tile_id()

    if str(file).endswith('.hdf'):
        from ..collections.hdf import to_geotiff

        opts = dict(prefix=Config.CUBES_DATA_DIR, path_include_month=path_include_month)

        asset_item_prefix = Config.CUBES_ITEM_PREFIX
        prefix = data_prefix
        opts['prefix'] = prefix

        tile_id = tile_id.replace('h', '0').replace('v', '0')
        destination = data.path(collection, **opts)
        destination.mkdir(parents=True, exist_ok=True)

        band_map = {
            b.name: dict(nodata=float(b.nodata),
                         min_value=float(b.min_value),
                         max_value=float(b.max_value))

            for b in collection.bands
        }

        item_result = to_geotiff(file, temporary_dir.name, band_map=band_map)
        files = dict()

        if item_result.files:
            ref = list(item_result.files.values())[0]
            srid = get_epsg_srid(str(ref))

            geom = from_shape(raster_extent(str(ref)), srid=4326)

            with rasterio.open(ref) as d:
                nodata = d.profile.get('nodata')
            # Trust in band metadata (no data)
            convex_hull = raster_convexhull(str(ref), no_data=nodata)

            if convex_hull.area > 0.0:
                convex_hull = from_shape(convex_hull, srid=4326)

        if kwargs.get('publish_hdf'):
            # Generate Quicklook and append asset
            assets['asset'] = Item.create_asset_definition(
                href=_item_prefix(Path(file), prefix=Config.CUBES_DATA_DIR, item_prefix=Config.CUBES_ITEM_PREFIX),
                mime_type=guess_mime_type(file),
                role=['data'],
                file=str(file)
            )

            file_band_map = item_result.files
        else:
            for _band, _geotiff in item_result.files.items():
                destination_path = destination / Path(_geotiff).name
                shutil.move(str(_geotiff), str(destination_path))
                files[_band] = destination_path

        file = destination
        cloud_cover = item_result.cloud_cover
    else:
        files = {}
        if not is_compressed:
            files = data.get_files(collection, path=file)

    items_to_publish = kwargs['activity'].get('items_to_publish')

    extra_assets = data.get_assets(collection, path=file)

    if items_to_publish:

        if is_compressed:
            extra_assets = data.get_assets(collection, path=temporary_dir.name)

        assets.pop('asset')
        assets.pop('thumbnail', None)

        extra_assets['PVI'] = str(quicklook)

        for item in items_to_publish:

            files[item['name']] = list(tmp.rglob(item['pattern']))[0]


    tile = Tile.query().filter(
        Tile.name == tile_id,
        Tile.grid_ref_sys_id == collection.grid_ref_sys_id
    ).first()

    collection_band_map = {b.name: b for b in collection.bands}

    for band_name, file in files.items():
        path = Path(file)
        file = str(file)

        # TODO: Define way to identify raster to support others collection
        is_raster = path.suffix.lower() in ('.tif', '.jp2')

        if is_raster:

            filename = re.sub('(MSIL1C|MSIL2A)', band_name, destination.name)

            target_file = destination.parent / f'{filename}.tif'

            if band_name not in ('AOT', 'WVP'):
                generate_cogs(file, target_file)

                if str(target_file) != file:
                    os.remove(file)

                if band_name in extra_assets:
                    extra_assets[band_name] = str(target_file)
                file = str(target_file)
                path = target_file
                srid = get_epsg_srid(str(target_file))
            else:
                logging.warning(f'Skipping cog for {band_name}')

            if srid == DEFAULT_SRID:
                srid = get_epsg_srid(str(target_file))

            if is_sen2cor_flag:
                link_file_name = os.path.basename(str(target_file))

                for res in [10, 20, 60]:
                    link_file_name = link_file_name.replace(f'_{res}m', '')

                link_file = destination / link_file_name
                relative_file = Path(target_file).relative_to(destination)

                if link_file.is_symlink():
                    link_file.unlink()

                os.symlink(str(relative_file), str(link_file))

        for band in collection.bands:
            if band.name == band_name:
                file_band_map[band.name] = file

                if geom is None or convex_hull is None:
                    geom = from_shape(raster_extent(file), srid=4326)
                    # Trust in band metadata (no data)
                    convex_hull = raster_convexhull(file, no_data=band.nodata)

                    if convex_hull.area > 0.0:
                        convex_hull = from_shape(convex_hull, srid=4326)

                assets[band.name] = _asset_definition(path, band, is_raster, cog=True, item_prefix=asset_item_prefix, prefix=prefix)

                break

    if extra_assets:
        for asset_name, asset_file in extra_assets.items():
            asset_file_path = Path(asset_file)

            suffix = asset_file_path.suffix

            filename = re.sub('(MSIL1C|MSIL2A)', asset_name, destination.name)

            asset_file_path = destination.parent / Path(filename+suffix)

            shutil.move(asset_file, str(asset_file_path))

            is_raster = asset_file_path.suffix.lower() in ('.tif',)

            if is_raster:
                compress_raster(str(asset_file_path), str(asset_file_path))

            if asset_file_path.suffix.lower() in ('.jp2',):

                asset_file_path_tif = destination.parent / f'{asset_file_path.stem}.tif'

                generate_cogs(str(asset_file_path), str(asset_file_path_tif))

                if str(asset_file_path) != str(asset_file_path_tif):
                    os.remove(str(asset_file_path))

                asset_file_path = asset_file_path_tif

            asset_definition_params = dict(
                path=asset_file_path,
                is_raster=is_raster,
                cog=False,
                item_prefix=asset_item_prefix,
                prefix=prefix
            )

            if asset_name == 'PVI':

                asset_definition_params.update(dict(role=['thumbnail']))

            assets[asset_name] = _asset_definition(**asset_definition_params)

    index_bands = generate_band_indexes(scene_id, collection, file_band_map)

    for band_name, band_file in index_bands.items():
        path = Path(band_file)

        assets[band_name] = _asset_definition(path, collection_band_map[band_name], is_raster=True, cog=True, item_prefix=asset_item_prefix, prefix=prefix)

    # TODO: Remove un-necessary files
    if is_sen2cor_flag:
        quicklook = Path(destination) / f'{scene_id}.png'
        generate_quicklook_pvi(destination, quicklook)
        relative_quicklook = _item_prefix(quicklook, item_prefix=asset_item_prefix, prefix=prefix)
        assets['thumbnail'] = Item.create_asset_definition(
            href=relative_quicklook,
            mime_type=guess_mime_type(str(quicklook)),
            role=['thumbnail'],
            file=str(quicklook)
        )

    if collection.quicklook and not is_sen2cor_flag:
        try:
            collection_bands = {b.id: b.name for b in collection.bands}

            red_file = file_band_map[collection_bands[collection.quicklook[0].red]]

            with rasterio.open(str(red_file)) as red_ds:
                nodata = red_ds.profile.get('nodata')
                if nodata is None:
                    _band_ref = collection_band_map[collection_bands[collection.quicklook[0].red]]
                    nodata = _band_ref.nodata

            green_file = file_band_map[collection_bands[collection.quicklook[0].green]]
            blue_file = file_band_map[collection_bands[collection.quicklook[0].blue]]

            quicklook = Path(destination) / f'{scene_id}.png'

            create_quick_look(str(quicklook), red_file, green_file, blue_file, no_data=nodata)

            relative_quicklook = _item_prefix(quicklook, item_prefix=asset_item_prefix, prefix=prefix)

            assets['thumbnail'] = Item.create_asset_definition(
                href=relative_quicklook,
                mime_type=guess_mime_type(str(quicklook)),
                role=['thumbnail'],
                file=str(quicklook)
            )
        except Exception as e:
            logging.warning(f'Could not generate quicklook for {scene_id} due {str(e)}')

    provider = Provider.query().filter(Provider.id == provider_id).first()

    if not geom and scene_meta:
        geofootprint = scene_meta.get("GeoFootprint")
        if geofootprint:
            shapely_geom = shapely.geometry.shape(geofootprint)
            convex_hull = from_shape(shapely_geom, srid=4326)
            geom = from_shape(shapely_geom.envelope, srid=4326)

            # TODO: Log files/bands which was not published.

    with db.session.begin_nested():
        item_defaults = dict(
            start_date=data.parser.sensing_date(),
            end_date=data.parser.sensing_date()
        )

        where = dict(name=scene_id, collection_id=collection.id)
        item, created = get_or_create_model(Item, defaults=item_defaults, **where)
        # When data already exists, mark "updated" as now.
        if not created:
            item.updated = datetime.datetime.utcnow()

        item.assets = assets
        item.cloud_cover = cloud_cover
        item.bbox = geom
        item.srid = srid
        item.footprint = convex_hull
        item.provider = provider
        item.is_available = True
        item.updated = datetime.datetime.utcnow()
        if scene_meta:
            item.metadata_ = scene_meta

        if tile is not None:
            item.tile_id = tile.id

        item.save(commit=False)

    db.session.commit()

    logging.info(f'Cleaning up temporary {temporary_dir.name}')
    shutil.rmtree(temporary_dir.name)

    if quicklook:
        _rm_dir(str(quicklook.parent))

    if not kwargs.get("keep_source"):
        logging.info(f"Removing source {str(destination)}")
        shutil.rmtree(destination)

    return item


def _rm_dir(directory):
    try:
        os.rmdir(directory)
    except:
        pass