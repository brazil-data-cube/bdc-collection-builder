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

- `GDAL <https://gdal.org/>`_ ``Version 2+``: make sure that ``gdal-config`` is installed and available in the ``PATH``.

- `Brazil Data Cube Database Module <https://github.com/brazil-data-cube/bdc-db>`_

- `Brazil Data Cube Core Module <https://github.com/brazil-data-cube/bdc-core>`_


Development Installation
------------------------

Clone the software repository:

.. code-block:: shell

    $ git clone https://github.com/brazil-data-cube/bdc-collection-builder.git


Go to the source code folder:

.. code-block:: shell

    $ cd bdc-collection-builder


Install in development mode:

.. code-block:: shell

    $ pip3 install -e .[all]

.. note::

    If you have problems during the GDAL Python package installation, please, make sure to have the GDAL library support installed in your system with its command line tools.


    You can check the GDAL version with:

    .. code-block:: shell

        $ gdal-config --version


    Then, if you want to install a specific version (example: 2.4.2), try:

    .. code-block:: shell

        $ pip install "gdal==2.4.2"


    If you still having problems with GDAL installation, you can generate a log in order to check what is happening with your installation. Use the following pip command:

    .. code-block:: shell

        $ pip install --verbose --log my.log "gdal==2.4.2"


    For more information, please, see [#f1]_ e [#f2]_.


.. note::

    If you have problems during the ``librabbitmq`` install with ``autoreconf``, please, install the ``autoconf`` build system. In Debian based systems (Ubuntu), you can install ``autoconf`` with:

    .. code-block:: shell

        $ sudo apt install autoconf


    For more information, please, see [#f3]_.


Generate the documentation:

.. code-block:: shell

    $ python setup.py build_sphinx


The above command will generate the documentation in HTML and it will place it under:

.. code-block:: shell

    doc/sphinx/_build/html/


Running in Development Mode
---------------------------

Launch Redis, RabbitMQ and PostgreSQL Containers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``docker-compose`` command can be used to launch the Redis and RabbitMQ containers:

.. code-block:: shell

    $ docker-compose up -d redis mq postgres


Let's take a look at each parameter in the above command:

    - ``up``: tells docker-compose to launch the containers.

    - ``-d``: tells docker-compose that containers will run in detach mode (as a deamon).

    - ``redis``: the name of a service in the ``docker-compose.yml`` file with all information to prepare a Redis container.

    - ``mq``: the name of a service in the ``docker-compose.yml`` file with all information to prepare a RabbitMQ container.

    - ``postgres``: the name of a service in the ``docker-compose.yml`` file with all information to prepare a PostgreSQL container.


.. note::

    Since docker-compose will map the services to the default system ports on localhost, make sure you are not running Redis, RabbitMQ or PostgreSQL on those ports in your system, otherwise you will have a port conflict during the attempt to launch the new containers.


.. note::

    If you have a PostgreSQL DBMS you can ommit the ``postgres`` service in the above command.


.. note::

    After launching the containers, check if they are up and running:

    .. code-block:: shell

        $ docker container ls

        CONTAINER ID        IMAGE                   COMMAND                  CREATED             STATUS              PORTS                                                                                        NAMES
        8c94877e7017        rabbitmq:3-management   "docker-entrypoint.s…"   34 seconds ago      Up 26 seconds       4369/tcp, 5671/tcp, 0.0.0.0:5672->5672/tcp, 15671/tcp, 25672/tcp, 0.0.0.0:15672->15672/tcp   bdc-collection-builder-rabbitmq
        acc51ff02295        mdillon/postgis         "docker-entrypoint.s…"   34 seconds ago      Up 24 seconds       0.0.0.0:5432->5432/tcp                                                                       bdc-collection-builder-pg
        84bae6370fbb        redis                   "docker-entrypoint.s…"   34 seconds ago      Up 27 seconds       0.0.0.0:6379->6379/tcp                                                                       bdc-collection-builder-redis



Prepare the Database System
~~~~~~~~~~~~~~~~~~~~~~~~~~~

You will need an instance of a PostgreSQL DBMS with a database prepared with the Collection Builder schema.


The following steps will show how to prepare the data model:


**1.** Create a PostgreSQL database and enable the PostGIS extension:

.. code-block:: shell

    SQLALCHEMY_DATABASE_URI=postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc \
    bdc-collection-builder db create-db


**2.** After that, run Flask-Migrate command to prepare the Collection Builder data model:

.. code-block:: shell

    SQLALCHEMY_DATABASE_URI=postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc \
    bdc-collection-builder db upgrade


Loading Demo Data
~~~~~~~~~~~~~~~~~

Load default fixtures of Brazil Data Cube data model:

Once the database is updated, we have prepared a command utility on Brazil Data Cube Database module to load some collection examples:

.. code-block:: shell

    SQLALCHEMY_DATABASE_URI=postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc \
    bdc-db fixtures init


Launching Sen2Cor and LaSRC 1.3.0
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before launching Sen2Cor and LaSRC processors, please, read the `CONFIG.rst <./CONFIG.rst>`_ documentation and make sure you have the right layout of auxiliary data in your filesystem.


If you have all the auxiliary data, use ``docker-compose`` to launch the surface reflectance processors as Docker containers:

.. code-block:: shell

    docker-compose up -d sen2cor espa-science


Then, check if all containers are up and running:

.. code-block:: shell

    $ docker container ls

    CONTAINER ID   IMAGE                                   COMMAND                  CREATED              STATUS              PORTS                                                                                        NAMES
    7af90085acd4   registry.dpi.inpe.br/rc_espa-science    "/entrypoint.sh pyth…"   About a minute ago   Up About a minute   0.0.0.0:5032->5032/tcp                                                                       bdc-collection-builder-espa-science
    ab58e9f6a7a3   registry.dpi.inpe.br/rc_sen2cor:2.8.0   "python rc_sen2cor.py"   About a minute ago   Up About a minute   0.0.0.0:5031->5031/tcp, 9764/tcp                                                             bdc-collection-builder-sen2cor
    8c94877e7017   rabbitmq:3-management                   "docker-entrypoint.s…"   4 days ago           Up 23 hours         4369/tcp, 5671/tcp, 0.0.0.0:5672->5672/tcp, 15671/tcp, 25672/tcp, 0.0.0.0:15672->15672/tcp   bdc-collection-builder-rabbitmq
    acc51ff02295   mdillon/postgis                         "docker-entrypoint.s…"   4 days ago           Up 23 hours         0.0.0.0:5432->5432/tcp                                                                       bdc-collection-builder-pg
    84bae6370fbb   redis                                   "docker-entrypoint.s…"   4 days ago           Up 23 hours         0.0.0.0:6379->6379/tcp                                                                       bdc-collection-builder-redis


Launching Collection Builder Workers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**1.** In order to launch the worker responsible for downloading data, run the following ``Celery`` command:

.. code-block:: shell

    $ DATA_DIR="/home/gribeiro/data/bdc-collection-builder" \
      SQLALCHEMY_DATABASE_URI="postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc" \
      SQLALCHEMY_DATABASE_URI_AWS="postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc" \
      REDIS_URL="redis://localhost:6379" \
      RABBIT_MQ_URL="pyamqp://guest@localhost" \
      celery -A bdc_collection_builder.celery.worker:celery worker -l INFO --concurrency 4 -Q download


As soon as the worker is launched, it will present a message like:

.. code-block:: shell

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
      . bdc_collection_builder.collections.landsat.tasks.atm_correction_landsat
      . bdc_collection_builder.collections.landsat.tasks.download_landsat
      . bdc_collection_builder.collections.landsat.tasks.publish_landsat
      . bdc_collection_builder.collections.landsat.tasks.upload_landsat
      . bdc_collection_builder.collections.sentinel.tasks.atm_correction
      . bdc_collection_builder.collections.sentinel.tasks.download_sentinel
      . bdc_collection_builder.collections.sentinel.tasks.publish_sentinel
      . bdc_collection_builder.collections.sentinel.tasks.upload_sentinel

    [2020-04-30 08:51:18,737: INFO/MainProcess] Connected to amqp://guest:**@127.0.0.1:5672//
    [2020-04-30 08:51:18,746: INFO/MainProcess] mingle: searching for neighbors
    [2020-04-30 08:51:20,040: INFO/MainProcess] mingle: all alone
    [2020-04-30 08:51:20,075: INFO/MainProcess] celery@enghaw-dell-note ready.



**2.** To launch the worker responsible for surface reflection generation (L2A processor based on Sen2Cor or LaSRC for Landsat 8), use the following ``Celery`` command:

.. code-block:: shell

    $ DATA_DIR="/home/gribeiro/data/bdc-collection-builder" \
      SQLALCHEMY_DATABASE_URI="postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc" \
      SQLALCHEMY_DATABASE_URI_AWS="postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc" \
      REDIS_URL="redis://localhost:6379" \
      RABBIT_MQ_URL="pyamqp://guest@localhost" \
      ESPA_URL="http://localhost:5032" \
      SEN2COR_URL="http://localhost:5031" \
      celery -A bdc_collection_builder.celery.worker:celery worker -l INFO --concurrency 4 -Q atm-correction


As soon as the worker is launched, it will present a message like:

.. code-block:: shell

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
      . bdc_collection_builder.collections.landsat.tasks.atm_correction_landsat
      . bdc_collection_builder.collections.landsat.tasks.download_landsat
      . bdc_collection_builder.collections.landsat.tasks.publish_landsat
      . bdc_collection_builder.collections.landsat.tasks.upload_landsat
      . bdc_collection_builder.collections.sentinel.tasks.atm_correction
      . bdc_collection_builder.collections.sentinel.tasks.download_sentinel
      . bdc_collection_builder.collections.sentinel.tasks.publish_sentinel
      . bdc_collection_builder.collections.sentinel.tasks.upload_sentinel

    [2020-04-30 08:53:57,977: INFO/MainProcess] Connected to amqp://guest:**@127.0.0.1:5672//
    [2020-04-30 08:53:58,055: INFO/MainProcess] mingle: searching for neighbors
    [2020-04-30 08:53:59,389: INFO/MainProcess] mingle: all alone
    [2020-04-30 08:53:59,455: WARNING/MainProcess] /home/gribeiro/Devel/github/brazil-data-cube/bdc-collection-builder/venv/lib/python3.7/site-packages/kombu/pidbox.py:74: UserWarning: A node named celery@enghaw-dell-note is already using this process mailbox!

    Maybe you forgot to shutdown the other node or did not do so properly?
    Or if you meant to start multiple nodes on the same host please make sure
    you give each node a unique node name!

      warnings.warn(W_PIDBOX_IN_USE.format(node=self))
    [2020-04-30 08:53:59,457: INFO/MainProcess] celery@enghaw-dell-note ready.


**3.** To launch the worker responsible for publishing the generated surface reflection data products, use the following ``Celery`` command:

.. code-block:: shell

    $ DATA_DIR="/home/gribeiro/data/bdc-collection-builder" \
      SQLALCHEMY_DATABASE_URI="postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc" \
      SQLALCHEMY_DATABASE_URI_AWS="postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc" \
      REDIS_URL="redis://localhost:6379" \
      RABBIT_MQ_URL="pyamqp://guest@localhost" \
      celery -A bdc_collection_builder.celery.worker:celery worker -l INFO --concurrency 4 -Q publish


As soon as the worker is launched, it will present a message like:

.. code-block:: shell

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
      . bdc_collection_builder.collections.landsat.tasks.atm_correction_landsat
      . bdc_collection_builder.collections.landsat.tasks.download_landsat
      . bdc_collection_builder.collections.landsat.tasks.publish_landsat
      . bdc_collection_builder.collections.landsat.tasks.upload_landsat
      . bdc_collection_builder.collections.sentinel.tasks.atm_correction
      . bdc_collection_builder.collections.sentinel.tasks.download_sentinel
      . bdc_collection_builder.collections.sentinel.tasks.publish_sentinel
      . bdc_collection_builder.collections.sentinel.tasks.upload_sentinel

    [2020-04-30 08:54:19,361: INFO/MainProcess] Connected to amqp://guest:**@127.0.0.1:5672//
    [2020-04-30 08:54:19,400: INFO/MainProcess] mingle: searching for neighbors
    [2020-04-30 08:54:20,504: INFO/MainProcess] mingle: all alone
    [2020-04-30 08:54:20,595: WARNING/MainProcess] /home/gribeiro/Devel/github/brazil-data-cube/bdc-collection-builder/venv/lib/python3.7/site-packages/kombu/pidbox.py:74: UserWarning: A node named celery@enghaw-dell-note is already using this process mailbox!

    Maybe you forgot to shutdown the other node or did not do so properly?
    Or if you meant to start multiple nodes on the same host please make sure
    you give each node a unique node name!

      warnings.warn(W_PIDBOX_IN_USE.format(node=self))
    [2020-04-30 08:54:20,602: INFO/MainProcess] celery@enghaw-dell-note ready.


Launching Collection Builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To launch the ``Flask`` application responsible for orchestrating the collection builder components, use the following command:

.. code-block:: shell

    $ DATA_DIR="/home/gribeiro/data/bdc-collection-builder" \
      SQLALCHEMY_DATABASE_URI="postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc" \
      SQLALCHEMY_DATABASE_URI_AWS="postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc" \
      REDIS_URL="redis://localhost:6379" \
      RABBIT_MQ_URL="pyamqp://guest@localhost" \
      bdc-collection-builder run


As soon as the ``Flask`` application is up and running, it will present a message like:

.. code-block:: shell

     * Environment: production
       WARNING: This is a development server. Do not use it in a production deployment.
       Use a production WSGI server instead.
     * Debug mode: off
     * Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)



Using the Collection Builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Please, refer to the document `USING.rst <./USING.rst>`_ for information on how to use the collection builder to download and generate surface reflectance data products.



.. rubric:: Footnotes

.. [#f1]

    During GDAL installation, if you have a build message such as the one showed below:

    .. code-block:: shell

        Skipping optional fixer: ws_comma
        running build_ext
        building 'osgeo._gdal' extension
        creating build/temp.linux-x86_64-3.7
        creating build/temp.linux-x86_64-3.7/extensions
        x86_64-linux-gnu-gcc -pthread -Wno-unused-result -Wsign-compare -DNDEBUG -g -fwrapv -O2 -Wall -g -fstack-protector-strong -Wformat -Werror=format-security -g -fwrapv -O2 -g -fstack-protector-strong -Wformat -Werror=format-security -Wdate-time -D_FORTIFY_SOURCE=2 -fPIC -I../../port -I../../gcore -I../../alg -I../../ogr/ -I../../ogr/ogrsf_frmts -I../../gnm -I../../apps -I/home/gribeiro/Devel/github/brazil-data-cube/wtss/venv/include -I/usr/include/python3.7m -I. -I/usr/include -c extensions/gdal_wrap.cpp -o build/temp.linux-x86_64-3.7/extensions/gdal_wrap.o
        extensions/gdal_wrap.cpp:3168:10: fatal error: cpl_port.h: No such file or directory
         #include "cpl_port.h"
                  ^~~~~~~~~~~~
        compilation terminated.
        error: command 'x86_64-linux-gnu-gcc' failed with exit status 1
        Running setup.py install for gdal ... error
        Cleaning up...

    You can instruct ``pip`` to look at the right place for header files when building GDAL:

    .. code-block:: shell

        $ C_INCLUDE_PATH="/usr/include/gdal" \
          CPLUS_INCLUDE_PATH="/usr/include/gdal" \
          pip install "gdal==2.4.2"


.. [#f2]

    On Linux Ubuntu 18.04 LTS you can install GDAL 2.4.2 from the UbuntuGIS repository:

    1. Create a file named ``/etc/apt/sources.list.d/ubuntugis-ubuntu-ppa-bionic.list`` and add the following content:

    .. code-block:: shell

        deb http://ppa.launchpad.net/ubuntugis/ppa/ubuntu bionic main
        deb-src http://ppa.launchpad.net/ubuntugis/ppa/ubuntu bionic main


    2. Then add the following key:

    .. code-block:: shell

        $ sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 6B827C12C2D425E227EDCA75089EBE08314DF160


    3. Then, update your repository index:

    .. code-block:: shell

        $ sudo apt-get update


    4. Finally, install GDAL:

    .. code-block:: shell

        $ sudo apt-get install libgdal-dev=2.4.2+dfsg-1~bionic0


.. [#f3]

    During ``librabbitmq`` installation, if you have a build message such as the one showed below:

    .. code-block::

        ...
        Running setup.py install for SQLAlchemy-Utils ... done
        Running setup.py install for bdc-db ... done
        Running setup.py install for librabbitmq ... error
        ERROR: Command errored out with exit status 1:
         command: /home/gribeiro/Devel/github/brazil-data-cube/bdc-collection-builder/venv/bin/python3.7 -u -c 'import sys, setuptools, tokenize; sys.argv[0] = '"'"'/tmp/pip-install-1i7mp5js/librabbitmq/setup.py'"'"'; __file__='"'"'/tmp/pip-install-1i7mp5js/librabbitmq/setup.py'"'"';f=getattr(tokenize, '"'"'open'"'"', open)(__file__);code=f.read().replace('"'"'\r\n'"'"', '"'"'\n'"'"');f.close();exec(compile(code, __file__, '"'"'exec'"'"'))' install --record /tmp/pip-record-m9lm5kjn/install-record.txt --single-version-externally-managed --compile --install-headers /home/gribeiro/Devel/github/brazil-data-cube/bdc-collection-builder/venv/include/site/python3.7/librabbitmq
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


    You will need to install ``autoconf``:

    .. code-block:: shell

        $ sudo apt install autoconf