#!/usr/bin/env python

import os
from setuptools import find_packages, setup


tests_require = []


extras_require = {
    "docs": [
        'bdc-readthedocs-theme @ git+git://github.com/brazil-data-cube/bdc-readthedocs-theme.git#egg=bdc-readthedocs-theme',
        'Sphinx>=2.1.2',
    ],
    "tests": tests_require
}

g = {}
with open(os.path.join('bdc_scripts', 'manifest.py'), 'rt') as fp:
    exec(fp.read(), g)
    version = g['version']

setup(
    name='bdc-scripts',
    version=version,
    description='Brazilian Data Cube Scripts for Cube Generation',
    author='Admin',
    author_email='admin@admin.com',
    url='https://github.com/brazil-data-cube/bdc-scripts.git',
    packages=find_packages(),
    install_requires=[
        'Flask>=1.1.1',
        'Flask-Cors>=3.0.8',
        'flask-restplus>=0.13.0',
        'flask_bcrypt>=0.7.1',
        'Flask-Migrate>=2.5.2',
        'Flask-SQLAlchemy>=2.4.1',
        'GeoAlchemy2>=0.6.2',
        # Utils for database creation
        'SQLAlchemy-Utils>=0.34.2',
        'SQLAlchemy[postgresql]>=1.3.10',
        'redis>=3.3.11',
        'requests>=2.22.0',
        'GDAL>=2.3.3',
        'numpy>=1.17.2',
        'numpngw>=0.0.8',  # TODO: Review this dependency
        'scikit-image>=0.16.2',
        'bdc-core @ git+git://github.com/brazil-data-cube/bdc-core.git#egg=bdc-core',
        # TODO: Temporary workaround since kombu has fixed version
        'celery[librabbitmq]==4.3.0',
        'librabbitmq==2.0.0',
        'vine==1.3.0',
        'amqp==2.5.1',
    ],
    entry_points={
        'console_scripts': [
            'bdc-scripts = bdc_scripts.cli:cli'
        ]
    },
    extras_require=extras_require,
    tests_require=tests_require,
    include_package_data=True,
)
