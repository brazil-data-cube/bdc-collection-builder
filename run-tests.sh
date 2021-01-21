#!/usr/bin/env bash
#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

pydocstyle bdc_collection_builder && \
isort bdc_collection_builder tests setup.py --check-only --diff && \
check-manifest --ignore ".drone.yml,.readthedocs.yml" && \
pytest &&
sphinx-build -qnW --color -b doctest docs/sphinx/ docs/sphinx/_build/doctest
