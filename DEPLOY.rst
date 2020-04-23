..
    This file is part of Brazil Data Cube Collection Builder.
    Copyright (C) 2019-2020 INPE.

    Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.


Deploying
=========

Setting up Sentinel
-------------------

Account Creation
~~~~~~~~~~~~~~~~

**Copernicus**

In order to search and obtain images from Copernicus SciHub (e.g. Sentinel-2 images), users must have a registered account at: `<https://scihub.copernicus.eu/dhus/#/self-registration>`_ and confirm validation through email. This account may take a few days to be operational when using it in scripts.

These information should be inserted into secrets.json as the following:

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


**CREODIAS**

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



Setting up Auxiliary Data
~~~~~~~~~~~~~~~~~~~~~~~~~

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


Setting up Landsat
------------------

Account Creation
~~~~~~~~~~~~~~~~

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



Setting up Auxiliary Data
~~~~~~~~~~~~~~~~~~~~~~~~~

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


Docker Installation
-------------------

.. note::

    Since docker will map the services to the default system
    ports on localhost, make sure you are not running PostgreSQL,
    Redis or RabbitMQ on those ports in your system.


Use the following command to up local PostgreSQL + PostGIS, Redis and RabbitMQ instances:

.. code-block:: shell

        docker-compose up -d postgres mq redis


Configuration
-------------

Open and edit **docker-compose.yml** with the following variables:

1. **DATA_DIR** - Path to store collections.
2. **SQLALCHEMY_DATABASE_URI** - BDC Collection Builder Database URI.
3. **REDIS_URL** - URI to connect on Redis Instance.
4. **RABBIT_MQ_URL** - URI to connect on RabbitMQ protocol.
5. **CLIENT_SECRET_KEY** and **CLIENT_AUDIENCE** for OAuth2 Integration.


The following variables consists in integration with AWS:

1. **SQLALCHEMY_DATABASE_URI_AWS** - Database URI to catalog on AWS.
2. **AWS_BUCKET_NAME** - AWS Bucket Name to store collections Surface Reflectance (SR).
3. **AWS_ACCESS_KEY_ID** and **AWS_SECRET_ACCESS_KEY** - AWS Credentials. You can generate in https://aws.amazon.com/pt/iam/.


.. note::

    Keep in mind that on invalid configuration for AWS instance will turn out in execution error on the following tasks
    related with surface reflectance products (SR): ``publish`` and ``upload``


Creating the Brazil Data Cube Data Model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**1.** Create a PostgreSQL database and enable the PostGIS extension:

.. code-block:: shell

        # Local
        SQLALCHEMY_DATABASE_URI=postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc \
        bdc-collection-builder db create-db
        # URI DB AWS
        SQLALCHEMY_DATABASE_URI=postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc_aws \
        bdc-collection-builder db create-db


**2.** After that, run Flask-Migrate command to prepare the Brazil Data Cube Collection Builder data model:

.. code-block:: shell

        # Local
        SQLALCHEMY_DATABASE_URI=postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc \
        bdc-collection-builder db upgrade
        # URI DB AWS
        SQLALCHEMY_DATABASE_URI=postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc_aws \
        bdc-collection-builder db upgrade

**3.** Once database is updated, we have prepared command utility on Brazil Data Cube Database module:

.. code-block:: shell

        # Local
        SQLALCHEMY_DATABASE_URI=postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc \
        bdc-db fixtures init
        # URI DB AWS
        SQLALCHEMY_DATABASE_URI=postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc_aws \
        bdc-db fixtures init


Updating an Existing Data Model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: shell

        SQLALCHEMY_DATABASE_URI=postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc \
        bdc-collection-builder db upgrade


Updating the Migration Scripts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: shell

        SQLALCHEMY_DATABASE_URI=postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc \
        bdc-collection-builder db migrate



HTTP Server and Workers
-----------------------


Once everything configured properly, use the following command to start HTTP server:

.. code-block:: shell

        docker-compose up -d


Dispatch Sentinel
~~~~~~~~~~~~~~~~~

.. code-block:: shell

        curl -XPOST -H "Content-Type: application/json" \
            --data '{"w": -46.40, "s": -13.1, "n": -13, "e": -46.3, "satsen": "S2", "start": "2019-01-01", "end": "2019-01-05", "cloud": 90, "action": "start"}' \
            localhost:5000/api/radcor/

Dispatch Landsat-8
~~~~~~~~~~~~~~~~~~


.. code-block:: shell

        curl -XPOST -H "Content-Type: application/json" \
            --data '{"w": -46.40, "s": -13.1, "n": -13, "e": -46.3, "satsen": "LC8", "start": "2019-01-01", "end": "2019-01-16", "cloud": 90, "action": "start"}' \
            localhost:5000/api/radcor/