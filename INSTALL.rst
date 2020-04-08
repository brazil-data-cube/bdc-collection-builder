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

    If you have problems during the GDAL package installation, please, make sure to have the GDAL library installed in your system with its command tools installed. You can check the GDAL version with: ``gdal-config --version``.


.. note::

    If you have problems during the ``librabbitmq`` install with ``autoreconf``, please, install the ``autoconf`` build system. In Debian based systems (Ubuntu), you can install ``autoconf`` with: ``sudo apt install autoconf``.



Generate the documentation:

.. code-block:: shell

        $ python setup.py build_sphinx


The above command will generate the documentation in HTML and it will place it under:

.. code-block:: shell

    doc/sphinx/_build/html/


Running in Development Mode
---------------------------

Launch Redis and RabbitMQ Containers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``docker-compose`` command can be used to launch the Redis and RabbitMQ containers:

.. code-block:: shell

        $ docker-compose up -d redis mq postgres


Let's take a look at each parameter in the above command:

    - ``up``: tells docker-compose to launch the containers.

    - ``-d``: tells docker-compose that containers will run in detach mode (as deamon).

    - ``redis``: the name of a service in the ``docker-compose.yml`` file with all information to prepare a Redis container.

    - ``mq``: the name of a service in the ``docker-compose.yml`` file with all information to prepare a RabbitMQ container.

    - ``postgres``: the name of a service in the ``docker-compose.yml`` file with all information to prepare a PostgreSQL container.


.. note::

    Since docker-compose will map the services to the default system ports on localhost, make sure you are not running Redis, RabbitMQ or PostgreSQL on those ports in your system, otherwise you will have a port conflict during the attempt to launch the new containers.


.. note::

    If you have a PostgreSQL DBMS you can ommit the ``postgres`` service in the above command.


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
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Load default fixtures of Brazil Data Cube data model:

Once the database is updated, we have prepared a command utility on Brazil Data Cube Database module to load some collection examples:

.. code-block:: shell

        SQLALCHEMY_DATABASE_URI=postgresql://postgres:bdc-collection-builder2019@localhost:5432/bdc \
        bdc-db fixtures init
