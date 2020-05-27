#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Define global functions used in Collection Builder."""


def initialize_factories():
    """Initialize Brazil Data Cube Collection Builder factories."""
    from .collections.landsat.utils import factory as landsat_factory
    from .collections.sentinel.utils import factory as sentinel_factory

    landsat_factory.register()
    sentinel_factory.register()


def finalize_factories():
    """Finalize the Collection Builder factories."""
    from .celery.cache import lock_handler

    lock_handler.release_all()
