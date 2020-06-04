..
    This file is part of Brazil Data Cube Collection Builder.
    Copyright (C) 2019-2020 INPE.

    Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.


======================
BDC Collection Builder
======================

.. image:: https://img.shields.io/badge/license-MIT-green
        :target: https://github.com/brazil-data-cube/bdc-collection-builder/blob/v0.2.0/LICENSE
        :alt: Software License

.. image:: https://readthedocs.org/projects/bdc-collection-builder/badge/?version=b-0.2
        :target: https://bdc-collection-builder.readthedocs.io/en/b-0.2/
        :alt: Documentation Status

.. image:: https://img.shields.io/badge/lifecycle-experimental-orange.svg
        :target: https://www.tidyverse.org/lifecycle/#experimental
        :alt: Software Life Cycle

.. image:: https://img.shields.io/github/tag/brazil-data-cube/bdc-collection-builder.svg
        :target: https://github.com/brazil-data-cube/bdc-collection-builder/releases
        :alt: Release

.. image:: https://badges.gitter.im/brazil-data-cube/community.svg/
        :target: https://gitter.im/brazil-data-cube/community#
        :alt: Join the chat


About
=====

BDC Collection Builder is a Python package for local server and AWS S3 bucket ingestion of satellite imagery, which contemplates:

- ``Download`` of Landsat-8/OLI and Sentinel-2A/B/MSI Level-1 products, from several image providers (USGS, Copernicus Sci-hub, ONDA, CREODIAS, AWS).

- ``Atmospheric correction`` processing of Level-1 products to generate surface reflectance (Level-2 products) through LaSRC and Sen2cor.

- ``Vegetation index calculation`` (NDVI and EVI) for both Level-1 and Level-2 products.

- ``Metadata publishing`` of Level-1 and Level-2 products on local or AWS database.


Installation
============

See `INSTALL.rst <./INSTALL.rst>`_.


Deploying
=========

See `DEPLOY.rst <./DEPLOY.rst>`_.


Developer Documentation
=======================

See https://bdc-collection-builder.readthedocs.io/en/b-0.2.


License
=======

.. admonition::
    Copyright (C) 2019-2020 INPE.

    Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.