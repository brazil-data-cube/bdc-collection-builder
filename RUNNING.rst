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

.. note::

    By default, the Brazil Data Cube Collection Builder uses OAuth2 client. If you don't have any
    OAuth2 credentials, make sure to comment the directives: **@require_oauth_scopes(scope="collection_builder:activities:POST")** and
    **@require_oauth_scopes(scope="collection_builder:activities:GET")** on ``bdc_collection_builder.collections.controller.py``.


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