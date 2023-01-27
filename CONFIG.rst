..
    This file is part of Brazil Data Cube Collection Builder.
    Copyright (C) 2022 INPE.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program. If not, see <https://www.gnu.org/licenses/gpl-3.0.html>.


Configuration
=============


.. _providers:

Setting up the Credentials for EO Data Providers
------------------------------------------------

The ``BDC Collection Builder`` uses `BDC-Collectors <https://github.com/brazil-data-cube/bdc-collectors.git>`_ to access and download
data from remote providers.


Copernicus (Sentinel 2A and 2B)
+++++++++++++++++++++++++++++++

.. note::

    This step is required if you would like to change credential for ``SciHub/ESA``.
    The commands make changes in database. Make sure you have exported
    ``SQLALCHEMY_DATABASE_URI=postgresql://<user>:<password>@<host>/<dbname>`` with
    proper values.


In order to search and obtain images from Copernicus SciHub (e.g. Sentinel-2A and 2B images), users must have a registered account at: `<https://scihub.copernicus.eu/dhus/#/self-registration>`_ and confirm validation through email. This account may take a few days to be operational when using it in scripts.

The ``BDC-Collection-Builder`` provide a command line :func:`bdc_collection_builder.cli.load_providers`.
Edit the ``credentials`` key in the file ``examples/data/providers/scihub.json`` with username and password. After that, load or edit credentials with command::

    bdc-collection-builder load-providers --ifile examples/data/providers/scihub.json --update


Once provider is ``created/updated``, you must attach it into collection as following::

    bdc-collection-builder set-provider --collection S2_L1C-1 --provider ESA


.. note::

    You must have a collection ``S2_L1C-1``. Please, check the ``Prepare the Database System`` in install step.
    The value ``ESA`` is the name of provider found in ``examples/data/providers/scihub.json``.


The collection ``S2_L1C-1`` will be marked as data origin from provider ``ESA``. You can check it with command::

    bdc-collection-builder overview --collection S2_L1C-1


The output::

    -> title: Sentinel-2 - MSI - Level-1C
    -> name: S2_L1C
    -> version: 1
    -> description: Level-1C product provides orthorectified Top-Of-Atmosphere (TOA) reflectance.
    -> collection_type: collection
    -> Providers:
      - ESA, driver=SciHub, priority=1, active=True


CREODIAS (Sentinel 2A and 2B)
+++++++++++++++++++++++++++++

.. note::

    This section is optional. It is an alternative to avoid the `Copernicus Long Term Archive <https://scihub.copernicus.eu/userguide/LongTermArchive>`_.

Due the `Copernicus Long Term Archive <https://scihub.copernicus.eu/userguide/LongTermArchive>`_, you may face issues while
requesting for download period higher than a year due Retention Policy. In this way, you can set a credential
to allow ``bdc-collection-builder`` download from `CREODIAS <https://creodias.eu/>`_ provider when scene is offline.

In order to search and obtain images from SciHub mirror CREODIAS, users must have a registered account at: https://creodias.eu/ and confirm validation through email.

Edit the ``credentials`` key in the file ``examples/data/providers/creodias.json`` with username and password. After that, load or edit credentials with command::

    bdc-collection-builder load-providers --ifile examples/data/providers/creodias.json --update


Once provider is ``created/updated``, you must attach it into collection as following::

    bdc-collection-builder set-provider --collection S2_L1C-1 --provider CREODIAS --priority 2


.. note::

    We are configuring ``CREODIAS`` with priority ``2``. In this case, it will be used as fallback if any error
    occurs in ``ESA``. The lowest value is for priority.


Now, if you show overview for collection ``S2_L1C-1``, the provider ``ESA`` and ``CREODIAS`` will be marked as data origin from provider::

    bdc-collection-builder overview --collection S2_L1C-1


The output::

    -> title: Sentinel-2 - MSI - Level-1C
    -> title: Sentinel-2 - MSI - Level-1C
    -> name: S2_L1C
    -> version: 1
    -> description: Level-1C product provides orthorectified Top-Of-Atmosphere (TOA) reflectance.
    -> collection_type: collection
    -> Providers:
      - ESA, driver=SciHub, priority=1, active=True
      - CREODIAS, driver=CREODIAS, priority=2, active=True


USGS (Landsat)
++++++++++++++


In order to search and obtain images from USGS Earth Explorer (e. g. Landsat-8 images), users must have a registered account at: `<https://ers.cr.usgs.gov/register/>`_ and confirm validation through email.


.. note::

    In the newest versions, the ``USGS`` provider account requires a ``Product Access Request`` which is available in
    `User Profile Access Request <https://ers.cr.usgs.gov/profile/access>`_. Fill out the form and wait for status
    ``Approved``.


Edit the ``credentials`` key in the file ``examples/data/providers/nasa-usgs.json`` with username and password. After that, load or edit credentials with command::

    bdc-collection-builder load-providers --ifile examples/data/providers/nasa-usgs.json --update


Once provider is ``created/updated``, you must attach it into collection as following::

    bdc-collection-builder set-provider --collection LC8_DN-1 --provider USGS --priority 1


Google Cloud Storage
++++++++++++++++++++

.. warning::

    Due unavailability of ``Landsat Collection 1``, this section is obsolete.
    Use ``Landsat Collection 2`` instead, which its only available in provider ``USGS`` (`EarthExplorer <https://earthexplorer.usgs.gov/>`_)


.. note::

    This section is optional and only works with product ``Landsat Collection 1``.


You must have a Google Account in order to use any ``Google Cloud Services``. In this way, you can create a new one in `Create a new Google Account <https://accounts.google.com/signup/v2>`_.

After that, you must also register an service account key in `Create a Service Account Key <https://console.cloud.google.com/apis/credentials/serviceaccountkey>`_ and download the service key.

You must set the environment variable ``GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account_key.json`` in order to enable the Google Provider in ``Collection Builder`` application.


Edit the ``GOOGLE_APPLICATION_CREDENTIALS`` key in the file ``examples/data/providers/google-datasets.json``. After that, load or edit it with command::

    bdc-collection-builder load-providers --ifile examples/data/providers/google-datasets.json --update


Once provider is ``created/updated``, you must attach it into collection as following::

    bdc-collection-builder set-provider --collection LC8_DN-1 --provider Google --priority 2



Remove attached provider from collection
----------------------------------------

You may need to detach a provider from collection if you having errors in collector as following::

    bdc-collection-builder set-provider --collection S2_L1C-1 --provider CREODIAS --remove


