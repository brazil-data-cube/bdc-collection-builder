#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Define utility for data retrieval from ONDA provider."""

import requests
from pathlib import Path
from ..utils import get_credentials


class OndaResult(dict):
    """Abstraction of ONDA Result data set.

    With this class, you can order and download a scene from ONDA provider.

    Note:
        This class does not wait til download is ready. You must implement
        own timer in order to check if scene is available.

    Examples:
        >>> catalog = search_onda_catalog_by_scene_id('S2A_MSIL1C_20170214T132231_N0204_R038_T23LPJ_20170214T132458')
        >>> catalog.order()
        >>> # When file is available
        >>> catalog.download('/tmp')
    """

    username: str = None
    password: str = None

    @property
    def id(self):
        """Retrieve ProductId of ONDA Result."""
        return self['id']

    @property
    def offline(self):
        """Retrieve metadata to identify downloadable scene."""
        return self['offline']

    @property
    def scene_id(self):
        """Retrieve scene_id from ONDA Result."""
        return self['name'].split('.')[0]

    def set_credentials(self, username, password):
        """Set credentials used to order a scene."""
        self.username = username
        self.password = password

    def download(self, destination: str):
        """Try to download scene from ONDA Provider.

        Remember that the scene may not be available. In this case, you must order
        using "OndaResult.order()". Make sure to set credentials.

        By default, when scene is offline, it will throw Exception.

        Args:
            destination: Path to store file
        """

        from bdc_collection_builder.collections.sentinel.download import _download

        base_uri = 'https://catalogue.onda-dias.eu/dias-catalogue/Products({})/$value'

        product_id = self.id

        auth = self.username, self.password

        req = requests.get(base_uri.format(product_id), stream=True, timeout=90, auth=auth)

        destination = Path(str(destination)) / '{}.zip'.format(self.scene_id)

        req.raise_for_status()

        with req:
            _download(str(destination), req)

    def order(self):
        """Order an offline product to ONDA Catalogue."""
        if self.offline:
            base_uri = 'https://catalogue.onda-dias.eu/dias-catalogue/Products({})/Ens.Order'

            product_id = self.id

            auth = self.username, self.password

            headers = {
                'Content-Type': 'application/json'
            }

            req = requests.post(base_uri.format(product_id), timeout=90, auth=auth, headers=headers)

            req.raise_for_status()

    def __str__(self) -> str:
        """Retrieve string representation of ONDA object."""
        return 'Onda(scene_id={}, offline={})'.format(self.scene_id, self.offline)

    def __repr__(self) -> str:
        """Retrieve string representation of ONDA object."""
        return self.__str__()


def search_onda_catalog(search, fmt='json') -> dict:
    """Search on ONDA Catalog."""
    base_uri = 'https://catalogue.onda-dias.eu/dias-catalogue/Products'

    query = {
        '$search': search,
        '$format': fmt
    }

    req = requests.get(base_uri, params=query, timeout=90)

    req.raise_for_status()

    content = req.json()

    return content


def search_onda_catalog_by_scene_id(scene_id: str) -> OndaResult:
    """Search on ONDA Catalogue for Sentinel 2 by scene_id."""
    results = search_onda_catalog('"name:{}.zip"'.format(scene_id))

    if len(results['value']) == 0:
        raise RuntimeError('{} not found.'.format(scene_id))

    return OndaResult(**results['value'][0])


def download_from_onda(scene_id: str, destination: str):
    """Download Sentinel 2 from ONDA <https://catalogue.onda-dias.eu/catalogue/>."""
    catalog = search_onda_catalog_by_scene_id(scene_id)

    credentials = get_credentials().get('onda')

    catalog.set_credentials(credentials.get('username'), credentials.get('password'))

    catalog.order()

    catalog.download(destination)
