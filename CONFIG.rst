..
    This file is part of Brazil Data Cube Collection Builder.
    Copyright (C) 2019-2020 INPE.

    Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.


Collection Builder Configuration
================================


Setting up the Credentials for EO Data Providers
------------------------------------------------


Copernicus (Sentinel 2A and 2B)
+++++++++++++++++++++++++++++++

In order to search and obtain images from Copernicus SciHub (e.g. Sentinel-2A and 2B images), users must have a registered account at: `<https://scihub.copernicus.eu/dhus/#/self-registration>`_ and confirm validation through email. This account may take a few days to be operational when using it in scripts.

The credentials should be inserted into ``secrets.json`` file as showned below:

.. code-block:: shell

    nano secrets.json


.. code-block:: json

    {
        "sentinel": {
            "USERNAME_HERE": {
                "password": "PASSWORD_HERE",
                "count": 2
            }
        }
    }


CREODIAS (Sentinel 2A and 2B)
+++++++++++++++++++++++++++++

.. note::

        This section is optional. It is an alternative to avoid the `Copernicus Long Term Archive <https://scihub.copernicus.eu/userguide/LongTermArchive>`_.

Due the `Copernicus Long Term Archive <https://scihub.copernicus.eu/userguide/LongTermArchive>`_, you may face issues while
requesting for download period higher than a year due Retention Policy. In this way, you can set a credential
to allow ``bdc-collection-builder`` download from `CREODIAS <https://creodias.eu/>`_ provider when scene is offline.

In order to search and obtain images from SciHub mirror CREODIAS, users must have a registered account at: https://creodias.eu/ and confirm validation through email.

After that, you can edit the file ``secrets.json`` as following:

.. code-block:: shell

        nano secrets.json


.. code-block:: json

    {
        "creodias": {
            "username": "CREODIAS_EMAIL",
            "password": "PASSWORD"
        }
    }


USGS (Landsat)
++++++++++++++


In order to search and obtain images from USGS Earth Explorer (e. g. Landsat-8 images), users must have a registered account at: `<https://ers.cr.usgs.gov/register/>`_ and confirm validation through email.


This information should be inserted into secrets.json

.. code-block:: shell

    nano secrets.json


.. code-block:: json

    {
        "landsat": {
            "username": "USGS_EMAIL",
            "password": "PASSWORD_HERE"
        }
    }


Setting up Auxiliary Data for Surface Reflectance Processors
------------------------------------------------------------


Sen2Cor
+++++++

Download *ESACCIC-LC for Sen2cor data package* auxiliary files from `<http://maps.elie.ucl.ac.be/CCI/viewer/download.php>`_.


Create a directory named ``/gfs/ds_data`` and extract the Sen2cor Auxiliary Data to the following directory.

.. code-block:: shell

        sudo mdkir -p /gfs/ds_data


The extracted files should be similar to:

.. code-block:: shell

        $ ls -lah /gfs/ds_data/CCI4SEN2COR/
        total 7.2G
        drwxrwxrwx 2 user user 4.0K Jan 14 13:41 .
        drwxrwxrwx 5 user user 4.0K Feb 28 08:15 ..
        -rwxrwxrwx 1 user user 4.0K Nov 23  2018 ._ESACCI-LC-L4-ALL-FOR-SEN2COR.tar
        -rwxrwxrwx 1 user user 6.0G Nov 23  2018 ESACCI-LC-L4-ALL-FOR-SEN2COR.tar
        -rw-r--r-- 1 user user 299M Dec 20  2017 ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7.tif
        -rwxrwxrwx 1 user user 4.0K Nov 23  2018 ._ESACCI-LC-L4-LCCS-WB-FOR-SEN2COR.tar
        -rwxrwxrwx 1 user user 595M Nov 23  2018 ESACCI-LC-L4-LCCS-WB-FOR-SEN2COR.tar
        -rw-r--r-- 1 user user 297M Dec 20  2017 ESACCI-LC-L4-WB-Map-150m-P13Y-2000-v4.0.tif
        -rw-rw-r-- 1 user user 2.6M Nov 22 16:57 GlobalSnowMap.tar.gz
        -rw-rw-r-- 1 user user  25M Nov 22 16:58 GlobalSnowMap.tiff


.. note::

    You can change ``/gfs/ds_data`` to your preference folder. Just keep in mind that you must edit the section
    ``sen2cor`` in the file ``docker-compose.yml``.


LasRC
+++++

Create a *auxiliaries* directory containing two folders: *L8* and *land_water_polygon*

.. code-block:: shell

        sudo mkdir -p /gfs/ds_data/auxiliaries/{L8,land_water_polygon}


Download the static land/water polygon from `<http://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/land_water_poly/land_no_buf.ply.gz>`_
into the *land_water_polygon* folder (for more details check `<https://github.com/USGS-EROS/espa-product-formatter>`_)

The folder ``land_water_polygon`` should be similar to:

.. code-block:: shell

        ls -lah /gfs/ds_data/auxiliaries/land_water_polygon/
        total 171M
        drwxrwxrwx 2 user user 4.0K Mar 26 00:21 .
        drwxrwxrwx 4 user user 4.0K Mar 19 03:41 ..
        -rw-rw-rw- 1 user user 171M Nov 26  2018 land_no_buf.ply


    Download the `<https://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/lasrc_auxiliary/L8/>`_ into *L8*.

.. note::

    The LADS folder can contain only data from dates which are going to be processed, instead of all the files.


The folder ``L8`` should be similar to:

.. code-block:: shell

        $ ls -lah /gfs/ds_data/auxiliaries/L8
        total 1.6G
        drwxrwxrwx  4 user user 4.0K Mar 19 03:41 .
        drwxrwxrwx  4 user user 4.0K Mar 19 03:41 ..
        -rw-rw-rw-  1 user user 124M Nov 26  2018 CMGDEM.hdf
        -rw-rw-rw-  1 user user 4.0K Nov 26  2018 ._.DS_Store
        -rw-rw-rw-  1 user user 8.1K Jul 15  2019 .DS_Store
        drwxrwxrwx 10 user user 4.0K Mar 19 03:41 LADS
        drwxrwxrwx  2 user user 4.0K Mar 19 04:45 LDCMLUT
        -rw-rw-rw-  1 user user 1.5G Nov 26  2018 ratiomapndwiexp.hdf
