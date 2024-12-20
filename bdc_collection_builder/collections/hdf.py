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

"""Module to deal with Hierarchical Data Format (HDF4/HDF5)."""

from pathlib import Path
from typing import Dict, NamedTuple

from osgeo import gdal

DTYPES = dict(
    uint8=gdal.GDT_Byte,
    int16=gdal.GDT_Int16,
    uint16=gdal.GDT_UInt16,
    int32=gdal.GDT_Int32,
    uint32=gdal.GDT_UInt32,
)

ItemResult = NamedTuple('ItemResult', [('files', dict), ('cloud_cover', float)])
"""Type to represent the extracted scenes from an Hierarchical Data Format (HDF4/HDF5)."""


def to_geotiff(hdf_path: str, destination: str, band_map: Dict[str, dict]) -> ItemResult:
    """Convert a Hierarchical Data Format (HDF4/HDF5) file to set of GeoTIFF files.

    Args:
        hdf_path (str) - Path to the HDF file to be extracted
        destination (str) - The destination folder.
        band_map (Dict[str, dict]) - The band map values for Datasets

    Note:
        The output GeoTIFF files are not Cloud Optimized GeoTIFF (COG).

    Tip:
        You may use the utility :meth:bdc_collection_builder.collections.utils.generate_cogs to generate Cloud Optimized GeoTIFF files.

    Raises:
        IOError When the input file is not a valid data set.

    Returns:
        ItemResult A struct containing the extracted files
    """
    data_set = gdal.Open(hdf_path)

    if data_set is None:
        raise IOError(f'Could not open {str(hdf_path)}')

    base_name = Path(hdf_path).stem
    metadata = data_set.GetMetadata()
    cloud_cover = float(metadata.get('QAPERCENTCLOUDCOVER.1') or 0)
    output_path = Path(destination)

    geotiff_driver = gdal.GetDriverByName('GTiff')
    files = dict()
    # Band iterator index to retrieve metadata value
    band_idx = 1
    for data_set_name, _ in data_set.GetSubDatasets():
        formal_name = data_set_name.split(":")[-1].replace('"', "")
        band_name = '_'.join(formal_name.split(' ')[3:])
        if not band_name and (base_name.startswith("MOD") or base_name.startswith("MYD")):
            band_name = formal_name

        data_set = gdal.Open(data_set_name)
        band = data_set.GetRasterBand(1)
        array = band.ReadAsArray()
        nodata = band.GetNoDataValue()
        if nodata is None:
            nodata = band_map.get(band_name)['nodata']

        tiff_file = output_path / f'{base_name}_{band_name}.tif'

        output_data_set = geotiff_driver.Create(
            str(tiff_file),
            data_set.RasterXSize,
            data_set.RasterYSize,
            1,
            DTYPES[array.dtype.name]
        )
        output_data_set_band = output_data_set.GetRasterBand(1)
        output_data_set.SetGeoTransform(data_set.GetGeoTransform())
        output_data_set.SetProjection(data_set.GetProjection())
        output_data_set.SetMetadata(metadata)
        output_data_set_band.WriteArray(array)
        output_data_set_band.SetNoDataValue(nodata)

        files[band_name] = str(tiff_file)

        output_data_set_band = None
        output_data_set = None

        band_idx += 1

    return ItemResult(files=files, cloud_cover=cloud_cover)


def is_valid(file_path: str) -> bool:
    """Check the HDF file integrity with GDAL library."""
    ds = gdal.Open(file_path)

    return ds is not None
