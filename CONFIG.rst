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


Google Cloud Storage
++++++++++++++++++++

.. note::

    This section is optional. But we strongly recommend the configuration due data availability.


You must have a Google Account in order to use any ``Google Cloud Services``. In this way, you can create a new one in `Create a new Google Account <https://accounts.google.com/signup/v2>`_.

After that, you must also register an service account key in `Create a Service Account Key <https://console.cloud.google.com/apis/credentials/serviceaccountkey>`_ and download the service key.

You must set the environment variable ``GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account_key.json`` in order to enable the Google Provider in ``Collection Builder`` application.


Setting up Auxiliary Data for Surface Reflectance Processors
------------------------------------------------------------


LaSRC 2
+++++++

.. note::

    The ``LaSRC 2`` is used to generate Surface Reflectance both ``Sentinel 2A/2B and Landsat-8`` collections.
    See more details `here <https://github.com/USGS-EROS/espa-surface-reflectance/releases/tag/surface_reflectance_dec2019>`_


Create a *ds_data* to store the *auxiliaries* data used by the processor LaSRC:

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


You have to download the folders ``LDCMLUT``, ``MSILUT`` and ``LADS``:

.. code-block:: shell

    wget -r --no-parent -nH --cut-dirs=4 https://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/lasrc_auxiliary/L8/LDCMLUT/
    wget -r --no-parent -nH --cut-dirs=4 https://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/lasrc_auxiliary/L8/MSILUT/
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
    drwxrwxrwx  2 user user 4.0K Mar 19 04:45 MSILUT
    -rw-rw-rw-  1 user user 1.5G Nov 26  2018 ratiomapndwiexp.hdf
