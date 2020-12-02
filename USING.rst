..
    This file is part of Brazil Data Cube Collection Builder.
    Copyright (C) 2019-2020 INPE.

    Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.


Using Collection Builder
========================

This section explains how to use the Collection Builder application to collect data from Sentinel and Landsat providers, how to use the surface reflectance processors, and how to publish the data sets.


If you have not read yet how to install or deploy the system, please refer to `INSTALL.rst <./INSTALL.rst>`_ or `DEPLOY.rst <./DEPLOY.rst>`_ documentation.

The resource `/api/radcor` is used to dispatch tasks for both data collect, processing and publish. These parameters can be defined as:

    - `w`, `s`, `a`, `d`: Bounding box limits;
    - `catalog`: Define the catalog to search;
    - `dataset`: The dataset name offered by `catalog`. See more in `BDC-Collectors  <https://github.com/brazil-data-cube/bdc-collectors>`_
    - `start`: Start datetime;
    - `end`: End datetime;
    - `cloud`: Cloud cover factor. Default is `100`;
    - `action`: Argument to dispatch execution. The supported values are:

        - `start` - which search and dispatches the matched scenes;
        - `preview` - search in catalog with given parameters and return the matched values;

    - `tasks`: Define the intent execution and which the collection to store data. The supported values are:

        - `download` - Tries to download data from remote server using the `bdc-collectors` and models `bdc.collections_providers` for download priorities;
        - `correction` - Apply Surface Reflectance Processor according the model `Collection.metadata` in `BDC-Catalog <https://bdc-catalog.readthedocs.io/en/latest/>`_;
        - `publish` - Publish the collection in database. This step also generates the `bdc.quicklook` and band indexes from `bdc.bands`;
        - `post` - Apply post processing step in datasets;
        - `harmonization` - Apply Data Harmonization on Landsat-5, Landsat-7, Landsat-8 and Sentinel-2 products using the module `sensor-harm <https://github.com/brazil-data-cube/sensor-harm>`_;

        The tasks parameter can be nested in order to given an order of execution. For example,
        if you need to `download` data, generates a Surface Reflectance (`correction`) and then publish data `publish`,
        you can chain the tasks as following:

        .. code-block::

            "tasks": [
                "type": "download",
                "collection": "LC8_DN",
                "args": {},
                "tasks": [
                    {
                        "type": "correction",
                        "collection": "LC8_SR",
                        "args": {},
                        "tasks": [
                            {
                                "type": "publish",
                                "collection": "LC8_SR",
                                "args": {},
                                "tasks": []
                            }
                        ]
                    }
                ]
            ]

        This will evaluate the tasks into `Celery Chain/Group Concept <https://docs.celeryproject.org/en/stable/userguide/canvas.html>`_.
        If you chain values into `tasks` property as array, the values are designed as `Celery Group`. Otherwise, as a `Celery Chain.`.


Collecting Sentinel 2A and 2B L1C Images
----------------------------------------

You can download a Sentinel 2 scene from the provider `SciHub` with dataset `S2MSI1C` using the following example:

.. code-block:: shell

        curl -XPOST -H "Content-Type: application/json" \
            --data '{
                "w": -45.9,
                "s": -12.74,
                "n": -12.6,
                "e": -45.8,
                "catalog": "SciHub",
                "dataset": "S2MSI1C",
                "start": "2020-01-09T00:00:00",
                "end": "2020-01-10T23:59:59",
                "cloud": 100,
                "action": "start",
                "tasks": [
                    {
                        "type": "download",
                        "collection": "S2_L1C",
                        "args": {},
                        "tasks": [
                            {
                                "type": "correction",
                                "collection": "S2_L2A_LASRC",
                                "args": {},
                                "tasks": [
                                    {
                                        "type": "publish",
                                        "collection": "S2_L2A_LASRC",
                                        "args": {}
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }' \
            localhost:5000/api/radcor/

The output of the above request can be seen below:

.. code-block:: js

    {
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
        },
        "Results": 1
    }


.. note::

    The parameter ``"action": "start"`` can be replaced by ``"action": "preview"`` in order to perform just a query in the provider.
    This option will not download the data but will show the found scenes in the provider.


You can check the status download container:

.. code-block:: shell

    docker logs -f bdc-collection-builder-worker-download --tail 200

    [2020-04-28 09:45:15,093: INFO/MainProcess] Received task: bdc_collection_builder.celery.tasks.download[5efed43b-b913-4877-b9e2-e97c3c9a8947]
    [2020-04-28 09:45:16,220: INFO/ForkPoolWorker-2] Starting Download S2A_MSIL1C_20200110T132231_N0208_R038_T23LLF_20200110T145523 - bdc020...
    [2020-04-28 09:45:17,598: INFO/ForkPoolWorker-2] Downloading image https://scihub.copernicus.eu/apihub/odata/v1/Products('9e16c509-06d5-4387-81e6-8d4f08f2ad72')/$value in /home/gribeiro/data/bdc-collection-builder/Repository/Archive/S2_MSI/2020-01/S2A_MSIL1C_20200110T132231_N0208_R038_T23LLF_20200110T145523.zip, user AtomicUser(bdc020, released=False), size 813 MB


Collecting Landsat-8 Level 1 Images
-----------------------------------

You can download a Landsat-8 scene using the following example:

.. code-block:: shell

        curl -XPOST -H "Content-Type: application/json" \
            --data '{
                "w": -48.33171,
                "s": -14.06716,
                "e": -46.21973,
                "n": -11.95925,
                "catalog": "USGS",
                "dataset": "LANDSAT_8_C1",
                "start": "2020-02-20T00:00:00",
                "end": "2020-02-23T23:59:59",
                "cloud": 100,
                "action": "start",
                "tasks": [
                    {
                        "type": "download",
                        "collection": "LC8_DN",
                        "args": {},
                        "tasks": [
                            {
                                "type": "correction",
                                "collection": "LC8_SR",
                                "args": {},
                                "tasks": [
                                    {
                                        "type": "publish",
                                        "collection": "LC8_SR",
                                        "args": {}
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }' \
            localhost:5000/api/radcor/


Output:

.. code-block:: shell

    {
        "notile-2020-01-01-2020-02-28": {
            "LC08_L1TP_221069_20200223_20200313_01_T1": {
                "sceneid": "LC08_L1TP_221069_20200223_20200313_01_T1",
                "scene_id": "LC82210692020054LGN00",
                "cloud": 56,
                "date": "2020-02-23",
                "wlon": -48.33171,
                "slat": -14.06716,
                "elon": -46.21973,
                "nlat": -11.95925,
                "path": "221",
                "row": "069",
                "resolution": 30,
                "link": "https://earthexplorer.usgs.gov/download/12864/LC82210692020054LGN00/STANDARD/EE",
                "icon": "https://landsat-pds.s3.amazonaws.com/c1/L8/221/069/LC08_L1TP_221069_20200223_20200313_01_T1/LC08_L1TP_221069_20200223_20200313_01_T1_thumb_large.jpg"
            }
        },
        "Results": 1
    }


Restart a task
--------------

The resource `/api/radcor/restart` is responsible for restart any tasks in `BDC-Collection-Builder`.


Restart by status
~~~~~~~~~~~~~~~~~

TODO


Restart by identifier
~~~~~~~~~~~~~~~~~~~~~

In order to restart a failed task in Collection Builder, you must get the activity identifier (``id``) on the table ``collection_builder.activities``.

For example, if you need to restart a Sentinel 2 download task which sceneid is ``S2A_MSIL1C_20200110T132231_N0208_R038_T23LLF_20200110T145523``, use the following commands:

Connect to database in docker:

.. code-block:: shell

    docker exec -it bdc-collection-builder-pg psql -U postgres -d bdc

Use the following command to search by activity type ``downloadS2`` and sceneid ``S2A_MSIL1C_20200110T132231_N0208_R038_T23LLF_20200110T145523``:

.. code-block:: sql

    SELECT id, activity_type, collection_id, sceneid FROM collection_builder.activities
     WHERE activity_type = 'downloadS2'
       AND sceneid = 'S2A_MSIL1C_20200110T132231_N0208_R038_T23LLF_20200110T145523'


    SELECT id, activity_type, collection_id, sceneid FROM collection_builder.activities
     WHERE activity_type = 'publishS2'
       AND sceneid = 'S2A_MSIL1C_20200110T132231_N0208_R038_T23LLF_20200110T145523'



After that, use the ``id`` to restart a collection builder activity:

.. code-block:: shell

    curl -XGET -H  "Content-Type: application/json" localhost:5000/api/radcor/restart?ids=1


.. note::

    If activity does not exists on database, you must dispatch a execution as mentioned in
    section `Collecting Sentinel 2A and 2B L1C Images`_ and `Collecting Landsat-8 Level 1 Images`_.
