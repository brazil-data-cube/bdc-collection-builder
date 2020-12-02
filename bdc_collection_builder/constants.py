#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Define all the constants values for BDC-Collection-Builder."""

from .version import __version__

COG_MIME_TYPE = 'image/tiff; application=geotiff; profile=cloud-optimized'

APPLICATION = dict(
    name='Collection Builder',
    uri='https://github.com/brazil-data-cube/bdc-collection-builder',
    version=__version__
)
