..
    This file is part of Brazil Data Cube Collection Builder.
    Copyright (C) 2019-2020 INPE.

    Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.


Installation
============

The Brazil Data Cube Collection Builder (``bdc-collection-builder``) depends essentially on:

- `Flask <https://palletsprojects.com/p/flask/>`_

- `SQLAlchemy <https://www.sqlalchemy.org/>`_

- `Redis <https://redis.io/>`_

- `Celery <http://www.celeryproject.org/>`_

- `RabbitMQ <https://www.rabbitmq.com/>`_

- `Brazil Data Cube Catalog Module <https://github.com/brazil-data-cube/bdc-catalog>`_

- `Brazil Data Cube Collectors <https://github.com/brazil-data-cube/bdc-collectors>`_

- `Sensor Harmonization <https://github.com/brazil-data-cube/sensor-harm>`_ (Optional)


Compatibility
-------------

Before deploy/install ``BDC-Collection-Builder``, please, take a look into compatibility table:

+------------------------+-------------+----------------+
| BDC-Collection-Builder | BDC-Catalog | BDC-Collectors |
+========================+=============+================+
| 0.8.x                  | 0.8.2       | 0.6.0          |
+------------------------+-------------+----------------+
| 0.6.x                  | 0.8.2       | 0.2.1          |
+------------------------+-------------+----------------+
| 0.4.x                  | 0.2.x       | ``NaN``        |
+------------------------+-------------+----------------+

Development Installation
------------------------


Clone the software repository::

    $ git clone https://github.com/brazil-data-cube/bdc-collection-builder.git


Go to the source code folder::

    $ cd bdc-collection-builder


Install in development mode::

    $ pip3 install -e .[docs,tests]


.. note::

    If you have problems during the ``librabbitmq`` install with ``autoreconf``, please, install the ``autoconf`` build system. In Debian based systems (Ubuntu), you can install ``autoconf`` with::

        $ sudo apt install autoconf


    For more information, please, see [#f1]_.

.. note::

    If you would like to publish ``Hierarchical Data Format`` (HDF) datasets, you may install the extra ``gdal``.
    Optionally, you can install all dependencies as following::

        $ pip3 install -e .[all]

    Make sure you have GDAL installed and available in ``PATH``.


Generate the documentation::

    $ python setup.py build_sphinx


The above command will generate the documentation in HTML and it will place it under::

    docs/sphinx/_build/html/


Optionally, you can serve these files temporally on ``http://localhost:8000`` using the following command::

    cd docs/sphinx/_build/html/
    python3 -m http.server


Running in Development Mode
---------------------------

Launch Redis, RabbitMQ and PostgreSQL Containers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``docker-compose`` command can be used to launch the Redis and RabbitMQ containers::

    $ docker-compose up -d redis mq postgres


Let's take a look at each parameter in the above command:

    - ``up``: tells docker-compose to launch the containers.

    - ``-d``: tells docker-compose that containers will run in detach mode (as a daemon).

    - ``redis``: the name of a service in the ``docker-compose.yml`` file with all information to prepare a Redis container.

    - ``mq``: the name of a service in the ``docker-compose.yml`` file with all information to prepare a RabbitMQ container.

    - ``postgres``: the name of a service in the ``docker-compose.yml`` file with all information to prepare a PostgreSQL container.


.. note::

    Since docker-compose will map the services to the default system ports on localhost,
    make sure you are not running Redis, RabbitMQ or PostgreSQL on those ports in your system,
    otherwise you will have a port conflict during the attempt to launch the new containers.


.. note::

    If you have a PostgreSQL DBMS you can omit the ``postgres`` service in the above command.


.. note::

    After launching the containers, check if they are up and running::

        $ docker container ls

        CONTAINER ID        IMAGE                   COMMAND                  CREATED             STATUS              PORTS                                                                                        NAMES
        8c94877e7017        rabbitmq:3-management   "docker-entrypoint.s…"   34 seconds ago      Up 26 seconds       4369/tcp, 5671/tcp, 0.0.0.0:5672->5672/tcp, 15671/tcp, 25672/tcp, 0.0.0.0:15672->15672/tcp   bdc-collection-builder-rabbitmq
        acc51ff02295        mdillon/postgis         "docker-entrypoint.s…"   34 seconds ago      Up 24 seconds       0.0.0.0:5432->5432/tcp                                                                       bdc-collection-builder-pg
        84bae6370fbb        redis                   "docker-entrypoint.s…"   34 seconds ago      Up 27 seconds       0.0.0.0:6379->6379/tcp                                                                       bdc-collection-builder-redis



Prepare the Database System
~~~~~~~~~~~~~~~~~~~~~~~~~~~

You will need an instance of a PostgreSQL DBMS with a database prepared with the Collection Builder schema.


The following steps will show how to prepare the data model:


**1.** Create a PostgreSQL database and enable the PostGIS extension::

    SQLALCHEMY_DATABASE_URI=postgresql://postgres:postgres@localhost:5432/bdc \
    bdc-db db init


**2.** Create extension `PostGIS`::

    SQLALCHEMY_DATABASE_URI=postgresql://postgres:postgres@localhost:5432/bdc \
    bdc-db db create-extension-postgis

**3.** Create table namespaces::

    SQLALCHEMY_DATABASE_URI=postgresql://postgres:postgres@localhost:5432/bdc \
    bdc-db db create-namespaces


**4.** After that, run Flask-Migrate command to prepare the Collection Builder data model::

    SQLALCHEMY_DATABASE_URI=postgresql://postgres:postgres@localhost:5432/bdc \
    bdc-collection-builder alembic upgrade


**5.** Load `BDC-Catalog` triggers with command::

    SQLALCHEMY_DATABASE_URI=postgresql://postgres:postgres@localhost:5432/bdc \
    bdc-db db create-triggers


**6.** Load `BDC-Collectors` data providers::

    SQLALCHEMY_DATABASE_URI=postgresql://postgres:postgres@localhost:5432/bdc \
    bdc-db db load-scripts


.. note::

    For a initial data of collections, we have prepared a minimal command line utility to load ``examples/data`` definitions.
    You can check out with the following command::

        SQLALCHEMY_DATABASE_URI=postgresql://postgres:postgres@localhost:5432/bdc \
        bdc-collection-builder load-data --from-dir examples/data # or individual as --ifile examples/data/sentinel-2-l1.json

    If you would like to link a collection with a default provider (``S2_L1C-1`` with ``SciHub``) use the command::

        SQLALCHEMY_DATABASE_URI=postgresql://postgres:postgres@localhost:5432/bdc \
        bdc-collection-builder set-provider --collection S2_L1C-1 --provider SciHub



.. note::

    Please refer to :doc:`config` the section
    ``Setting up the Credentials for EO Data Providers`` to set valid access credentials for data providers.


Prepare the containers Sen2Cor and LaSRC 1.3.0
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before launching Sen2Cor and LaSRC processors, please, read the :doc:`config` documentation and make sure you have the right layout of auxiliary data in your filesystem.


If you have all the auxiliary data, edit `docker-compose.yml` the section `atm-correction` and fill the following configuration based in the directory where auxiliaries are stored::

    # LaSRC / LEDAPS
    - "LASRC_AUX_DIR=/path/to/landsat/auxiliaries/L8"
    - "LEDAPS_AUX_DIR=/path/to/landsat/ledaps_auxiliaries"
    # Sen2Cor
    - "SEN2COR_AUX_DIR=/path/to/sen2cor/CCI4SEN2COR"
    - "SEN2COR_CONFIG_DIR=/path/to/sen2cor/config/2.8"


.. note::

    Remember that these variables are relative inside container. You may change the mount volume in the section `volumes`.

    The 'SEN2COR_CONFIG_DIR` is base configuration of Sen2Cor instance with folder `cfg` and file `L2A_GIPP.xml`.


Launching Collection Builder Workers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**1.** In order to launch the worker responsible for downloading data, run the following ``Celery`` command::

    $ DATA_DIR="/home/user/data/bdc-collection-builder" \
      SQLALCHEMY_DATABASE_URI="postgresql://postgres:postgres@localhost:5432/bdc" \
      REDIS_URL="redis://localhost:6379" \
      RABBIT_MQ_URL="pyamqp://guest@localhost" \
      celery -A bdc_collection_builder.celery.worker:celery worker -l INFO --concurrency 2 -Q download


As soon as the worker is launched, it will present a message like:

.. code-block::

     -------------- celery@enghaw-dell-note v4.4.2 (cliffs)
    --- ***** -----
    -- ******* ---- Linux-5.3.0-46-generic-x86_64-with-Ubuntu-18.04-bionic 2020-04-30 08:51:18
    - *** --- * ---
    - ** ---------- [config]
    - ** ---------- .> app:         bdc_collection_builder:0x7fa166e9a490
    - ** ---------- .> transport:   amqp://guest:**@localhost:5672//
    - ** ---------- .> results:     postgresql://postgres:**@localhost:5432/bdc
    - *** --- * --- .> concurrency: 4 (prefork)
    -- ******* ---- .> task events: OFF (enable -E to monitor tasks in this worker)
    --- ***** -----
     -------------- [queues]
                    .> download         exchange=download(direct) key=download


    [tasks]
      . bdc_collection_builder.celery.tasks.correction
      . bdc_collection_builder.celery.tasks.download
      . bdc_collection_builder.celery.tasks.harmonization
      . bdc_collection_builder.celery.tasks.post
      . bdc_collection_builder.celery.tasks.publish

    [2020-04-30 08:51:18,737: INFO/MainProcess] Connected to amqp://guest:**@127.0.0.1:5672//
    [2020-04-30 08:51:18,746: INFO/MainProcess] mingle: searching for neighbors
    [2020-04-30 08:51:20,040: INFO/MainProcess] mingle: all alone
    [2020-04-30 08:51:20,075: INFO/MainProcess] celery@enghaw-dell-note ready.



**2.** To launch the worker responsible for surface reflection generation (L2A processor based on Sen2Cor or LaSRC for Landsat 8), use the following ``Celery`` command::

    $ DATA_DIR="/home/user/data/bdc-collection-builder" \
      SQLALCHEMY_DATABASE_URI="postgresql://postgres:postgres@localhost:5432/bdc" \
      REDIS_URL="redis://localhost:6379" \
      RABBIT_MQ_URL="pyamqp://guest@localhost" \
      LASRC_AUX_DIR=/path/to/auxiliaries/L8 \
      LEDAPS_AUX_DIR=/path/to/auxiliaries/ledaps \
      celery -A bdc_collection_builder.celery.worker:celery worker -l INFO --concurrency 4 -Q correction


As soon as the worker is launched, it will present a message like:

.. code-block::

     -------------- celery@enghaw-dell-note v4.4.2 (cliffs)
    --- ***** -----
    -- ******* ---- Linux-5.3.0-46-generic-x86_64-with-Ubuntu-18.04-bionic 2020-04-30 08:53:57
    - *** --- * ---
    - ** ---------- [config]
    - ** ---------- .> app:         bdc_collection_builder:0x7ff25bff5390
    - ** ---------- .> transport:   amqp://guest:**@localhost:5672//
    - ** ---------- .> results:     postgresql://postgres:**@localhost:5432/bdc
    - *** --- * --- .> concurrency: 4 (prefork)
    -- ******* ---- .> task events: OFF (enable -E to monitor tasks in this worker)
    --- ***** -----
     -------------- [queues]
                    .> atm-correction   exchange=atm-correction(direct) key=atm-correction


    [tasks]
      . bdc_collection_builder.celery.tasks.correction
      . bdc_collection_builder.celery.tasks.download
      . bdc_collection_builder.celery.tasks.harmonization
      . bdc_collection_builder.celery.tasks.post
      . bdc_collection_builder.celery.tasks.publish

    [2020-04-30 08:53:57,977: INFO/MainProcess] Connected to amqp://guest:**@127.0.0.1:5672//
    [2020-04-30 08:53:58,055: INFO/MainProcess] mingle: searching for neighbors
    [2020-04-30 08:53:59,389: INFO/MainProcess] mingle: all alone
    [2020-04-30 08:53:59,457: INFO/MainProcess] celery@enghaw-dell-note ready.

.. note::

    This configuration is only for LaSRC/LEDAPS with Fmask4. If you would like to run with Sen2Cor,
    check `CONFIG <./CONFIG.rst>`_.


**3.** To launch the worker responsible for publishing the generated surface reflection data products, use the following ``Celery`` command::

    $ DATA_DIR="/home/user/data/bdc-collection-builder" \
      SQLALCHEMY_DATABASE_URI="postgresql://postgres:postgres@localhost:5432/bdc" \
      REDIS_URL="redis://localhost:6379" \
      RABBIT_MQ_URL="pyamqp://guest@localhost" \
      celery -A bdc_collection_builder.celery.worker:celery worker -l INFO --concurrency 4 -Q publish


As soon as the worker is launched, it will present a message like:

.. code-block::

     -------------- celery@enghaw-dell-note v4.4.2 (cliffs)
    --- ***** -----
    -- ******* ---- Linux-5.3.0-46-generic-x86_64-with-Ubuntu-18.04-bionic 2020-04-30 08:54:19
    - *** --- * ---
    - ** ---------- [config]
    - ** ---------- .> app:         bdc_collection_builder:0x7f52d876e3d0
    - ** ---------- .> transport:   amqp://guest:**@localhost:5672//
    - ** ---------- .> results:     postgresql://postgres:**@localhost:5432/bdc
    - *** --- * --- .> concurrency: 4 (prefork)
    -- ******* ---- .> task events: OFF (enable -E to monitor tasks in this worker)
    --- ***** -----
     -------------- [queues]
                    .> publish          exchange=publish(direct) key=publish


    [tasks]
      . bdc_collection_builder.celery.tasks.correction
      . bdc_collection_builder.celery.tasks.download
      . bdc_collection_builder.celery.tasks.harmonization
      . bdc_collection_builder.celery.tasks.post
      . bdc_collection_builder.celery.tasks.publish

    [2020-04-30 08:54:19,361: INFO/MainProcess] Connected to amqp://guest:**@127.0.0.1:5672//
    [2020-04-30 08:54:19,400: INFO/MainProcess] mingle: searching for neighbors
    [2020-04-30 08:54:20,504: INFO/MainProcess] mingle: all alone
    [2020-04-30 08:54:20,602: INFO/MainProcess] celery@enghaw-dell-note ready.


.. note::

    In these examples, we have launched individual workers ``download``, ``atm-correction``,
    ``publish`` listening in different ``queues``.
    For convenience, you may set the parameter ``-Q download,atm-correction,publish`` to make the
    worker listen all these queues in runtime.
    Just make sure that the worker has the required variables for each kind of processing.


Launching Collection Builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To launch the ``Flask`` application responsible for orchestrating the collection builder components, use the following command::

    $ DATA_DIR="/home/user/data/bdc-collection-builder" \
      SQLALCHEMY_DATABASE_URI="postgresql://postgres:postgres@localhost:5432/bdc" \
      REDIS_URL="redis://localhost:6379" \
      RABBIT_MQ_URL="pyamqp://guest@localhost" \
      bdc-collection-builder run


As soon as the ``Flask`` application is up and running, it will present a message like::

     * Environment: production
       WARNING: This is a development server. Do not use it in a production deployment.
       Use a production WSGI server instead.
     * Debug mode: off
     * Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)



Usage
~~~~~

Please, refer to the document :doc:`usage` for information on how to use the collection builder to download and generate surface reflectance data products.



.. rubric:: Footnotes

.. [#f1]

    During ``librabbitmq`` installation, if you have a build message such as the one showed below:

    .. code-block::

        ...
        Running setup.py install for SQLAlchemy-Utils ... done
        Running setup.py install for bdc-db ... done
        Running setup.py install for librabbitmq ... error
        ERROR: Command errored out with exit status 1:
         command: /home/user/bdc-collection-builder/venv/bin/python3.7 -u -c 'import sys, setuptools, tokenize; sys.argv[0] = '"'"'/tmp/pip-install-1i7mp5js/librabbitmq/setup.py'"'"'; __file__='"'"'/tmp/pip-install-1i7mp5js/librabbitmq/setup.py'"'"';f=getattr(tokenize, '"'"'open'"'"', open)(__file__);code=f.read().replace('"'"'\r\n'"'"', '"'"'\n'"'"');f.close();exec(compile(code, __file__, '"'"'exec'"'"'))' install --record /tmp/pip-record-m9lm5kjn/install-record.txt --single-version-externally-managed --compile --install-headers /home/user/bdc-collection-builder/venv/include/site/python3.7/librabbitmq
             cwd: /tmp/pip-install-1i7mp5js/librabbitmq/
        Complete output (107 lines):
        /tmp/pip-install-1i7mp5js/librabbitmq/setup.py:167: DeprecationWarning: 'U' mode is deprecated
          long_description = open(os.path.join(BASE_PATH, 'README.rst'), 'U').read()
        running build
        - pull submodule rabbitmq-c...
        Cloning into 'rabbitmq-c'...
        Note: checking out 'caad0ef1533783729c7644a226c989c79b4c497b'.

        You are in 'detached HEAD' state. You can look around, make experimental
        changes and commit them, and you can discard any commits you make in this
        state without impacting any branches by performing another checkout.

        If you want to create a new branch to retain commits you create, you may
        do so (now or later) by using -b with the checkout command again. Example:

          git checkout -b <new-branch-name>

        - autoreconf
        sh: 1: autoreconf: not found
        - configure rabbitmq-c...
        /bin/sh: 0: Can't open configure


    You will need to install ``autoconf``::

        $ sudo apt install autoconf

