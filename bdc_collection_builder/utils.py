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
    from .celery.cache import cache
    from .collections.sentinel.clients import sentinel_clients

    cache.initialize()
    sentinel_clients.initialize()


def finalize_factories():
    """Finalize the Collection Builder factories."""
    from .celery.cache import lock_handler

    lock_handler.release_all()
