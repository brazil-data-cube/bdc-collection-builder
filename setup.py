#!/usr/bin/env python

import os
from setuptools import find_packages, setup


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
    'docs': [
        'bdc-readthedocs-theme @ git+git://github.com/brazil-data-cube/bdc-readthedocs-theme.git#egg=bdc-readthedocs-theme',
        'Sphinx>=2.1.2',
    ],
    "tests": tests_require
}

extras_require['all'] = [req for exts, reqs in extras_require.items() for req in reqs]

install_requires = [
    'beautifulsoup4>=4.8.1',
    'boto3>=1.11',
    'docutils>=0.10,<0.15'
    'Flask>=1.1.1',
    'Flask-Cors>=3.0,<4.0',
    'flask-restplus>=0.13.0',
    'Flask-Migrate>=2.5.2',
    'Flask-SQLAlchemy>=2.4.1',
    'GeoAlchemy2>=0.6.2',
    'marshmallow-sqlalchemy>=0.19.0',
    # Utils for database creation
    'SQLAlchemy-Utils>=0.34.2',
    'SQLAlchemy[postgresql]>=1.3.10',
    'rasterio>=1.1.2',
    'redis>=3.3.11',
    'requests>=2.22.0',
    'GDAL>=2.3.3',
    'numpy>=1.17.2',
    'numpngw>=0.0.8',  # TODO: Review this dependency
    'scikit-image>=0.16.2',
    'bdc-core @ git+git://github.com/brazil-data-cube/bdc-core.git#egg=bdc-core',
    'bdc-db @ git+git://github.com/brazil-data-cube/bdc-db.git@v0.2.0#egg=bdc-db',
    'stac @ git+git://github.com/brazil-data-cube/stac.py.git#egg=stac',
    'celery[librabbitmq]>=4.3.0',
    # TODO: Remove werkzeug dependency when https://github.com/noirbizarre/flask-restplus/issues/777 is fixed
    'Werkzeug>=0.16,<1.0'
]

g = {}
with open(os.path.join('bdc_collection_builder', 'version.py'), 'rt') as fp:
    exec(fp.read(), g)
    version = g['__version__']

setup(
    name='bdc-collection-builder',
    version=version,
    description='Brazil Data Cube for Collection Generation',
    author='Admin',
    author_email='admin@admin.com',
    url='https://github.com/brazil-data-cube/bdc-collection-builder.git',
    packages=find_packages(),
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'bdc-collection-builder = bdc_collection_builder.cli:cli'
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
