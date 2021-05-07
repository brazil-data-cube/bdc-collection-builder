..
    This file is part of Brazil Data Cube Collection Builder.
    Copyright (C) 2019-2020 INPE.

    Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.


=======
Changes
=======


Version 0.8.1 (2021-05-07)
--------------------------

- Fix dependency resolver for Docker images.
- Fix PosixPath entry while publish Landsat files `234 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/234>`_.


Version 0.8.0 (2021-05-04)
--------------------------

- Add route resource to check published scenes `225 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/225>`_.
- Add support to publish MODIS Cloud Optimized GeoTIFF (COG) data `221 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/221>`_.
- Add support to publish MODIS as HDF item `231 <https://github.com/brazil-data-cube/bdc-collection-builder/pull/231>`_.
- Change default compression type to deflate on COG Files `227 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/227>`_.
- Add support to publish Landsat-8 Collection 2 `220 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/220>`_.


Version 0.6.1 (2021-01-21)
--------------------------

- Skip publish scenes Level-1 when it already is done `#211 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/211>`_.
- Fix quicklook generation in publish collection Level-1 `#207 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/207>`_.
- Add support to download scene using collection tiles (MGRS/WRS2) `#212 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/212>`_.
- Migrate the collection builder utility for namespace creation to `BDC-DB 0.4.0 <https://bdc-db.readthedocs.io/en/latest/usage.html#command-line-interface-cli>`_ (`#215 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/215>`_).
- Add Drone CI support `#216 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/216>`_.



Version 0.6.0 (2020-12-03)
--------------------------

- Fix provider_id not being saved in `bdc.item' - provider_id `#202 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/202>`_, `#140 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/140>`_.
- Fix publish item transaction error `#87 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/87>`_.
- Fix wrong thumbnail path for Landsat products `#180 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/180>`_.
- Add support with `BDC-Catalog 0.6.4 <http://bdc-catalog.readthedocs.io/>`_, `#174 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/174>`_.
- Add support to change default name for collections `#182 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/182>`_.
- Add support with `BDC-Collectors <https://github.com/brazil-data-cube/bdc-collectors>`_ to search and collect data product from different providers `PR 187 <https://github.com/brazil-data-cube/bdc-collection-builder/pull/187>`_.
- Add Harmonization support (using extras `pip install -e .[harmonization]`) `#138 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/138>`_.
- Generate vegetation band indexes dynamically relying on `bdc.bands.metadata` `#164 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/164>`_, commit `0fe15de <https://github.com/brazil-data-cube/bdc-collection-builder/commit/0fe15debceb912144a995d82eb68a7a2b1595340>`_.


Version 0.4.1 (2020-09-08)
--------------------------

Bug fixes:

    - Fix Fmask4 re-sample in post-processing task for Sentinel - `#169 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/169>`_.

Changes:

    - Data synchronization with Amazon Simple Storage (S3) - `#170 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/170>`_


Version 0.4.0 (2020-08-25)
--------------------------

- Add `LaSRC 2.0 <https://github.com/USGS-EROS/espa-surface-reflectance>`_ with `FMask 4.2 <https://github.com/GERSL/Fmask>`_ on collections Landsat-8 and Sentinel-2 - `#156 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/156>`_
- Fix band name "quality" is wrong for Landsat 8 using LaSRC and Fmask - `#162 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/162>`_
- Set USGS EarthExplorer as default provider of Landsat instead AWS - `#144 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/144>`_
- Fix bug in sen2cor and publish on multiple scenes in the same sensing date - `#142 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/142>`_
- Skipping download of Landsat8 Real Time imagery (RT) - `#125 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/125>`_
- Fix the tar.gz validator on Collection Landsat-8 L1 - `#123 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/123>`_
- Improve the restart process - `#75 <https://github.com/brazil-data-cube/bdc-collection-builder/issues/75>`_

    - Restart a task by scene_id, status


Version 0.2.0 (2020-04-29)
--------------------------

- First experimental version.
- Metadata ingestion of Image Collections.
- Data download from open data catalogs:
  - `Copernicus <https://scihub.copernicus.eu/>`_.
  - `CREODIAS <https://creodias.eu/>`_.
  - `EarthExplorer <https://earthexplorer.usgs.gov/>`_.
- Processors for image collections based on:
  - `Sen2Cor <https://step.esa.int/main/third-party-plugins-2/sen2cor/>`_: processor for the generation of Sentinel-2 Level 2A product.
  - `LaSRC <https://github.com/USGS-EROS/espa-surface-reflectance>`_: processor for the generation of Landsat 8 surface reflectance data products.
- Multi-container applications based on ``Docker`` and ``docker-compose``.
- Documentation system based on ``Sphinx``.
- Documentation integrated to ``Read the Docs``.
- Package support through ``Setuptools``.
- Installation and deploy instructions.
- Schema versioning through ``Flask-Migrate``.
- Source code versioning based on `Semantic Versioning 2.0.0 <https://semver.org/>`_.
- License: `MIT <https://github.com/brazil-data-cube/bdc-collection-builder/blob/v0.2.0/LICENSE>`_.
