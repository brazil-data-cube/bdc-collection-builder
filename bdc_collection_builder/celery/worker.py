#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Defines a structure component to run celery worker."""

# Builder
from .. import create_app
from . import create_celery_app


app = create_app()
celery = create_celery_app(app)
