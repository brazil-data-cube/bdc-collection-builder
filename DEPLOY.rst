..
    This file is part of Brazil Data Cube Collection Builder.
    Copyright (C) 2019-2020 INPE.

    Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.


Deploying
=========


Configuration
-------------


Collector Providers
~~~~~~~~~~~~~~~~~~~

Please, refer to the section "Setting up the Credentials for EO Data Providers" in the `CONFIG.rst <./CONFIG.rst>`_ documentation.


Auxiliary Data for Atmospheric Correction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Please, refer to the section "Setting up Auxiliary Data for Surface Reflectance Processors" in the `CONFIG.rst <./CONFIG.rst>`_ documentation.


docker-compose.yml
~~~~~~~~~~~~~~~~~~

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


Running the Docker Containers
-----------------------------

.. note::

    If you do not have a PostgreSQL instance with the Brazil Data Cube data model up and running, you will need to prepare one before following the rest of this documentation.


    In order to launch a PostgreSQL container, you can rely on the docker-compose service file. The following command will start a new container with PostgreSQL:

    .. code-block:: shell

        $ docker-compose up -d postgres


    After launching the container, please, refer to the section "Prepare the Database System" in the `INSTALL.rst <./INSTALL.rst>`_ documentation. This will guide you in the preparation of the PostgreSQL setup.


.. note::

    Since docker will map the services to the default system
    ports on localhost, make sure you are not running PostgreSQL,
    Redis or RabbitMQ on those ports in your system.


Once everything is properly configured, use the following command to start all the services:

.. code-block:: shell

        docker-compose up -d


.. note::

    Refer to the `USING.rst <./USING.rst>`_ documentation in order to use the collection builder services.