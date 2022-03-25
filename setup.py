#!/usr/bin/env python
#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

import os

from setuptools import find_packages, setup

readme = open('README.rst').read()

history = open('CHANGES.rst').read()

docs_require = [
    'Sphinx>=2.1',
    'sphinx_rtd_theme',
    'sphinx-copybutton',
]

tests_require = [
    'check-manifest>=0.40',
    'coverage>=4.5',
    'coveralls>=1.8',
    'pydocstyle>=4.0',
    'pytest>=5.0.0,<6.0.0',
    'pytest-cov>=2.8',
    'pytest-pep8>=1.0',
    'isort>4.3',
]

extras_require = {
    'docs': docs_require,
    'tests': tests_require,
    'gdal': [
        'GDAL>=2.3',
    ]
}

extras_require['all'] = [req for exts, reqs in extras_require.items() for req in reqs]

setup_requires = [
    'pytest-runner>=5.2',
]

install_requires = [
    'beautifulsoup4>=4.8.1',
    'boto3>=1.11',
    'docutils>=0.10,<0.15'
    'Flask>=1.1.1',
    'marshmallow-sqlalchemy>=0.19.0',
    'rasterio>=1.1.2,<1.2',
    'rio-cogeo>=1.1,<2',
    'numpy>=1.17.2',
    'numpngw>=0.0.8',
    'scikit-image>=0.16.2',
    'SQLAlchemy[postgresql_psycopg2binary]>=1.3,<1.4',
    'bdc-collectors @ git+https://github.com/brazil-data-cube/bdc-collectors.git@v0.6.0#egg=bdc-collectors',
    'bdc-catalog @ git+https://github.com/brazil-data-cube/bdc-catalog.git@v0.8.2#egg=bdc-catalog',
    'celery[librabbitmq]>=4.3,<4.4.3',
    'python-dateutil>=2,<3',
    'Werkzeug>=0.16,<1.0',
    'shapely>=1.7,<2',
    'MarkupSafe==2.0.1',
]

packages = find_packages()

g = {}
with open(os.path.join('bdc_collection_builder', 'version.py'), 'rt') as fp:
    exec(fp.read(), g)
    version = g['__version__']

setup(
    name='bdc-collection-builder',
    version=version,
    description=__doc__,
    long_description=readme + '\n\n' + history,
    keywords='Brazil Data Cube Collection Builder Module',
    license='MIT',
    author='INPE',
    author_email='brazildatacube@dpi.inpe.br',
    url='https://github.com/brazil-data-cube/bdc-collection-builder',
    packages=packages,
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    entry_points={
        'console_scripts': [
            'bdc-collection-builder = bdc_collection_builder.cli:cli'
        ],
        'bdc_db.alembic': [
            'bdc_collection_builder = bdc_collection_builder:alembic'
        ],
        'bdc_db.models': [
            'celery = celery.backends.database.models',
            'bdc_collection_builder = bdc_collection_builder.collections.models',
        ],
        'bdc_db.namespaces': [
            'bdc_collection_builder = bdc_collection_builder.config:Config.ACTIVITIES_SCHEMA'
        ],
    },
    extras_require=extras_require,
    install_requires=install_requires,
    setup_requires=setup_requires,
    tests_require=tests_require,
    classifiers=[
        'Development Status :: 1 - Planning',
        'Environment :: Web Environment',
        'Intended Audience :: Education',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3 :: Only',
        'Topic :: Scientific/Engineering :: GIS',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)

