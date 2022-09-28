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

"""Module to deal with BDC-Collector integration."""

import logging
from typing import Any, List, Type

from bdc_catalog.models import db
from bdc_collectors.base import BaseProvider

from .models import CollectionProviderSetting, ProviderSetting
from .utils import get_provider_type


class DataCollector:
    """Data wrapper to store the given instance `bdc_catalog.models.Provider` and the data collector factory."""

    _db_provider: Any
    _provider: BaseProvider
    _collection_provider: Any

    def __init__(self, instance, provider: Type[BaseProvider], collection_provider: Any, **kwargs):
        """Create a data collector instance."""
        self._db_provider = instance

        if isinstance(instance.credentials, dict):
            copy_args = instance.credentials.copy()
            copy_args.update(**kwargs)

            self._provider = provider(**copy_args)
        else:
            self._provider = provider(*instance.credentials, **kwargs)

        self._collection_provider = collection_provider

    def __str__(self):
        """Retrieve String representation for DataCollector."""
        return f'DataCollector({self.provider_name})'

    @property
    def active(self) -> bool:
        """Retrieve the provider availability in database."""
        return self._collection_provider.active

    @property
    def priority(self) -> bool:
        """Retrieve the provider priority order in database."""
        return self._collection_provider.priority

    @property
    def instance(self):
        """Retrieve the database instance of bdc_catalog.models.Provider."""
        return self._db_provider

    @property
    def provider_name(self) -> str:
        """Retrieve the provider name."""
        return self._db_provider.driver_name

    def download(self, *args, **kwargs):
        """Download data from remote provider."""
        return self._provider.download(*args, **kwargs)

    def search(self, *args, **kwargs):
        """Search for dataset in the provider."""
        # TODO: Apply adapter in the results here??
        return self._provider.search(*args, **kwargs)


def get_provider_order(collection: Any, include_inactive=False, **kwargs) -> List[DataCollector]:
    """Retrieve a list of providers which the bdc_catalog.models.Collection is associated.

    Note:
        This method requires the initialization of extension `bdc_catalog.ext.BDCCatalog`.

    With a given collection, it seeks in `ProviderSetting`
    and `CollectionsProvidersSetting` association and then look
    for provider supported in the entry point `bdc_collectors.providers`.

    Args:
        collection - An instance of bdc_catalog.models.Collection
        include_inactive - List also the inactive providers. Default=False
        **kwargs - Extra parameters to pass to the Provider instance.
    Returns:
        A list of DataCollector, ordered by priority.
    """
    where = []

    if not include_inactive:
        where.append(CollectionProviderSetting.active.is_(True))

    collection_providers = (
        db.session
        .query(ProviderSetting, CollectionProviderSetting)
        .filter(
            CollectionProviderSetting.collection_id == collection.id,
            ProviderSetting.id == CollectionProviderSetting.provider_id,
            *where
        )
        .order_by(CollectionProviderSetting.priority.asc())
        .all()
    )

    result = []

    for collection_provider in collection_providers:
        provider_name = collection_provider.ProviderSetting.driver_name

        provider_class = get_provider_type(provider_name)

        if provider_class is None:
            logging.warning(f'The collection requires the provider {provider_name} but it is not supported.')
            continue

        collector = DataCollector(collection_provider.ProviderSetting,
                                  provider_class, collection_provider, **kwargs)
        result.append(collector)

    return result
