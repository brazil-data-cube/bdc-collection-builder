#!/usr/bin/env bash
#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

pydocstyle bdc_collection_builder && \
isort --check-only --diff --recursive bdc_collection_builder/*.py && \
check-manifest --ignore ".readthedocs.*" && \
pytest &&
sphinx-build -qnW --color -b doctest doc/sphinx/ doc/sphinx/_build/doctest
