#!/usr/bin/env python

import os
from setuptools import find_packages, setup


tests_require = []


extras_require = {
    'docs': [
        'bdc-readthedocs-theme @ git+git://github.com/brazil-data-cube/bdc-readthedocs-theme.git#egg=bdc-readthedocs-theme',
        'Sphinx>=2.1.2',
    ],
    "tests": tests_require
}

g = {}
with open(os.path.join('bdc_scripts', 'version.py'), 'rt') as fp:
    exec(fp.read(), g)
    version = g['__version__']

setup(
    name='bdc-scripts',
    version=version,
    description='Brazilian Data Cube Scripts for Cube Generation',
    author='Admin',
    author_email='admin@admin.com',
    url='https://github.com/brazil-data-cube/bdc-scripts.git',
    packages=find_packages(),
    install_requires=[
        'beautifulsoup4>=4.8.1',
        'Flask>=1.1.1',
        'flask-restplus>=0.13.0',
        'Flask-Migrate>=2.5.2',
        'Flask-SQLAlchemy>=2.4.1',
        'GeoAlchemy2>=0.6.2',
        'marshmallow-sqlalchemy>=0.19.0',
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
        'amqp==2.5.2',
    ],
    entry_points={
        'console_scripts': [
            'bdc-scripts = bdc_scripts.cli:cli'
        ]
    },
    extras_require=extras_require,
    tests_require=tests_require,
    include_package_data=True,
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
