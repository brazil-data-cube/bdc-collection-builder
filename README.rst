..
    This file is part of Brazil Data Cube Collection Builder.
    Copyright (C) 2022 INPE.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program. If not, see <https://www.gnu.org/licenses/gpl-3.0.html>.


======================
BDC Collection Builder
======================

.. image:: https://img.shields.io/badge/License-GPLv3-blue.svg
        :target: https://github.com/brazil-data-cube/bdc-collection-builder/blob/master/LICENSE
        :alt: Software License

.. image:: https://readthedocs.org/projects/bdc-collection-builder/badge/?version=latest
        :target: https://bdc-collection-builder.readthedocs.io/en/latest/
        :alt: Documentation Status

.. image:: https://img.shields.io/badge/lifecycle-maturing-blue.svg
        :target: https://www.tidyverse.org/lifecycle/#maturing
        :alt: Software Life Cycle

.. image:: https://img.shields.io/github/tag/brazil-data-cube/bdc-collection-builder.svg
        :target: https://github.com/brazil-data-cube/bdc-collection-builder/releases
        :alt: Release

.. image:: https://img.shields.io/discord/689541907621085198?logo=discord&logoColor=ffffff&color=7389D8
        :target: https://discord.com/channels/689541907621085198#
        :alt: Join us at Discord


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

See https://bdc-collection-builder.readthedocs.io/en/latest/.


License
=======

.. admonition::
    Copyright (C) 2022 INPE.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

