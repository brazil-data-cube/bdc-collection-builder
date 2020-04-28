..
    This file is part of Brazil Data Cube Collection Builder.
    Copyright (C) 2019-2020 INPE.

    Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.


Using Collection Builder
========================

This section explains how to use the Collection Builder application to collect data from Sentinel and Landsat providers, and how to ....


If you have not read yet how to install or deploy the system, please refer to `INSTALL.rst <./INSTALL.rst>`_ or `DEPLOY.rst <./DEPLOY.rst>`_ documentation.


Dispatch Sentinel
-----------------

You can download a Sentinel 2 scene using the following example:

.. code-block:: shell

        curl -XPOST -H "Content-Type: application/json" \
            --data '{
                "w": -46.40,
                "s": -13.1,
                "n": -13,
                "e": -46.3,
                "satsen": "S2",
                "start": "2020-01-01", "end": "2020-01-10",
                "cloud": 90,
                "action": "start"
            }' \
            localhost:5000/api/radcor/

Output:

.. code-block:: js

    {
        "notile-2020-01-01-2020-01-10": {
            "S2A_MSIL1C_20200110T132231_N0208_R038_T23LLF_20200110T145523": {
                "pathrow": "23LLF",
                "sceneid": "S2A_MSIL1C_20200110T132231_N0208_R038_T23LLF_20200110T145523",
                "type": "MSIL1C",
                "date": "2020-01-10",
                "cloud": 31.5879,
                "footprint": "MULTIPOLYGON (((-45.834015 -13.655483318755016, -45.830658 -12.662710833236419, -46.841522 -12.657636587187465, -46.84897 -13.649996915046348, -45.834015 -13.655483318755016)))",
                "tileid": "23LLF",
                "size": "813.67 MB",
                "link": "https://scihub.copernicus.eu/apihub/odata/v1/Products('9e16c509-06d5-4387-81e6-8d4f08f2ad72')/$value",
                "icon": "https://scihub.copernicus.eu/apihub/odata/v1/Products('9e16c509-06d5-4387-81e6-8d4f08f2ad72')/Products('Quicklook')/$value"
            }
        },
        "Results": 1
    }


You can check the status download container:

.. code-block:: shell

    docker logs -f bdc-collection-builder-worker-download --tail 200

    [2020-04-28 09:45:15,093: INFO/MainProcess] Received task: bdc_collection_builder.collections.sentinel.tasks.download_sentinel[5efed43b-b913-4877-b9e2-e97c3c9a8947]
    [2020-04-28 09:45:16,220: INFO/ForkPoolWorker-2] Starting Download S2A_MSIL1C_20200110T132231_N0208_R038_T23LLF_20200110T145523 - bdc020...
    [2020-04-28 09:45:17,598: INFO/ForkPoolWorker-2] Downloading image https://scihub.copernicus.eu/apihub/odata/v1/Products('9e16c509-06d5-4387-81e6-8d4f08f2ad72')/$value in /home/gribeiro/data/bdc-collection-builder/Repository/Archive/S2_MSI/2020-01/S2A_MSIL1C_20200110T132231_N0208_R038_T23LLF_20200110T145523.zip, user AtomicUser(bdc020, released=False), size 813 MB


Dispatch Landsat-8
------------------

You can download a Landsat-8 scene using the following example:

.. code-block:: shell

        curl -XPOST -H "Content-Type: application/json" \
            --data '{"w": -46.40, "s": -13.1, "n": -13, "e": -46.3, "satsen": "LC8", "start": "2019-01-01", "end": "2019-01-16", "cloud": 90, "action": "start"}' \
            localhost:5000/api/radcor/


Restart a task
--------------

In order to restart a failed task in Collection Builder, you must get the activity identifier (``id``) on the table ``collection_builder.activities``.

For example, if you need to restart a Sentinel 2 download task which sceneid is ``S2A_MSIL1C_20200110T132231_N0208_R038_T23LLF_20200110T145523`` you use the following commands:

Connect to database in docker:

.. code-block:: shell

    docker exec -it bdc-collection-builder-pg psql -U postgres -d bdc

Use the following command to search by activity type ``downloadS2`` and sceneid ``S2A_MSIL1C_20200110T132231_N0208_R038_T23LLF_20200110T145523``:

.. code-block:: sql

    SELECT id, activity_type, collection_id, sceneid FROM collection_builder.activities
     WHERE activity_type = 'downloadS2'
       AND sceneid = 'S2A_MSIL1C_20200110T132231_N0208_R038_T23LLF_20200110T145523'

After that, use the ``id`` to restart a collection builder activity:

.. code-block:: shell

    curl -XGET -H  "Content-Type: application/json" localhost:5000/api/radcor/restart?ids=1


.. note::

    If activity does not exists on database, you must dispatch a execution as mentioned in
    section `Dispatch Sentinel`_ and `Dispatch Landsat-8`_.