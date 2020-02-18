..
    This file is part of Python Module for BDC Collection Builder.
    Copyright (C) 2019-2020 INPE.

    BDC Collection Builder free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.


Running BDC Collection Builder
==============================

Setting up Sentinel
-------------------

Account Creation
~~~~~~~~~~~~~~~~

In order to search and obtain images from Copernicus SciHub (e.g. Sentinel-2 images), users must have a registered account at: https://scihub.copernicus.eu/dhus/#/self-registration and confirm validation through email. This account may take a few days to be operational when using it in scripts.


In order to search and obtain images from SciHub mirror CREODIAS, users must have a registered account at: https://creodias.eu/ and confirm validation through email.


These information should be inserted into secrets.json


Setting up Auxiliary Data
~~~~~~~~~~~~~~~~~~~~~~~~~

Download *ESACCIC-LC for Sen2cor data package* auxiliary files from ``http://maps.elie.ucl.ac.be/CCI/viewer/download.php``


Extract the files and mount Sen2cor Auxiliary Data volume to this directory.


Setting up Landsat
------------------

Account Creation
~~~~~~~~~~~~~~~~

In order to search and obtain images from USGS Earth Explorer (e. g. Landsat-8 images), users must have a registered account at: https://ers.cr.usgs.gov/register/ and confirm validation through email.


This information should be inserted into secrets.json


Setting up Auxiliary Data
~~~~~~~~~~~~~~~~~~~~~~~~~

Create a *auxiliaries* directory containing two folders: *L8* and *land_water_polygon*


Download the static land/water polygon from ``http://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/land_water_poly/land_no_buf.ply.gz`` into the *land_water_polygon* folder (for more details check ``https://github.com/USGS-EROS/espa-product-formatter``)


Download the ``https://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/lasrc_auxiliary/L8/`` into *L8*. The LADS folder can contain only data from dates which are going to be processed, instead of all the files.


.. code-block:: shell

        TODO


Running BDC Collection Builder in the Command Line
----------------------------------------

.. code-block:: shell

        TODO
