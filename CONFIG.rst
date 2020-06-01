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

The credentials should be inserted into ``secrets.json`` file as showed below:

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

Download **ESACCIC-LC for Sen2cor data package** (``ESACCI-LC-L4-ALL-FOR-SEN2COR.zip``) auxiliary files from `<http://maps.elie.ucl.ac.be/CCI/viewer/download.php>`_.

Create a directory named ``/gfs/ds_data`` and extract the Sen2cor Auxiliary Data to the following directory.

.. code-block:: shell

        sudo mdkir -p /gfs/ds_data
        cd /gfs/ds_data
        sudo wget ftp://geo10.elie.ucl.ac.be/v207/ESACCI-LC-L4-ALL-FOR-SEN2COR.zip
        sudo unzip ESACCI-LC-L4-ALL-FOR-SEN2COR.zip
        sudo wget https://storage.googleapis.com/cci-lc-v207/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7.zip
        sudo unzip ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7.zip
        sudo mv product/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7.tif CCI4SEN2COR/
        cd CCI4SEN2COR
        sudo wget ftp://geo10.elie.ucl.ac.be/CCI/ESACCI-LC-L4-WB-Map-150m-P13Y-2000-v4.0.tif



The extracted files should be similar to:

.. code-block:: shell

        $ ls -lah /gfs/ds_data/CCI4SEN2COR/
        total 7.2G
        drwxrwxrwx 2 user user 4.0K Jan 14 13:41 .
        drwxrwxrwx 5 user user 4.0K Feb 28 08:15 ..
        -rwxrwxrwx 1 user user 6.0G Nov 23  2018 ESACCI-LC-L4-ALL-FOR-SEN2COR.tar
        -rw-r--r-- 1 user user 299M Dec 20  2017 ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7.tif
        -rwxrwxrwx 1 user user 595M Nov 23  2018 ESACCI-LC-L4-LCCS-WB-FOR-SEN2COR.tar
        -rw-r--r-- 1 user user 297M Dec 20  2017 ESACCI-LC-L4-WB-Map-150m-P13Y-2000-v4.0.tif


.. note::

    You can change ``/gfs/ds_data`` to your preference folder. Just keep in mind that you must edit the section
    ``sen2cor`` in the file ``docker-compose.yml``.


LaSRC 1.3.0
+++++++++++

Create a *auxiliaries* directory containing two folders: *L8* and *land_water_polygon*:

.. code-block:: shell

        sudo mkdir -p /gfs/ds_data/auxiliaries/{L8,land_water_polygon}
        cd /gfs/ds_data/auxiliaries


Download the static land/water polygon from `<http://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/land_water_poly/land_no_buf.ply.gz>`_
into the *land_water_polygon* folder (for more details check `<https://github.com/USGS-EROS/espa-product-formatter>`_)

.. code-block:: shell

        cd /gfs/ds_data/auxiliaries/land_water_polygon
        sudo wget http://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/land_water_poly/land_no_buf.ply.gz
        sudo gunzip land_no_buf.ply.gz


The folder ``land_water_polygon`` should be similar to:

.. code-block:: shell

        ls -lah /gfs/ds_data/auxiliaries/land_water_polygon/
        total 171M
        drwxrwxrwx 2 user user 4.0K Mar 26 00:21 .
        drwxrwxrwx 4 user user 4.0K Mar 19 03:41 ..
        -rw-rw-rw- 1 user user 171M Nov 26  2018 land_no_buf.ply


Download the `<https://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/lasrc_auxiliary/L8/>`_ into *L8*.

.. code-block:: shell

        cd /gfs/ds_data/auxiliaries/L8
        wget https://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/lasrc_auxiliary/L8/CMGDEM.hdf
        wget https://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/lasrc_auxiliary/L8/ratiomapndwiexp.hdf


You have to download the folders ``LDCMLUT`` and ``LADS``:

.. code-block:: shell

        wget -r --no-parent -nH --cut-dirs=4 https://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/lasrc_auxiliary/L8/LDCMLUT/
        wget -r --no-parent -nH --cut-dirs=4 https://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/lasrc_auxiliary/L8/LADS/


.. note::

    The LADS folder can contain only data from dates which are going to be processed, instead of all the files.


After that, the folder ``L8`` should be similar to:

.. code-block:: shell

        $ ls -lah /gfs/ds_data/auxiliaries/L8
        total 1.6G
        drwxrwxrwx  4 user user 4.0K Mar 19 03:41 .
        drwxrwxrwx  4 user user 4.0K Mar 19 03:41 ..
        -rw-rw-rw-  1 user user 124M Nov 26  2018 CMGDEM.hdf
        drwxrwxrwx 10 user user 4.0K Mar 19 03:41 LADS
        drwxrwxrwx  2 user user 4.0K Mar 19 04:45 LDCMLUT
        -rw-rw-rw-  1 user user 1.5G Nov 26  2018 ratiomapndwiexp.hdf
