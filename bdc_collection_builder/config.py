#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Define config file for Brazil Data Cube Collection Builder."""

import os
import tempfile
from distutils.util import strtobool

CURRENT_DIR = os.path.dirname(__file__)


def get_settings(env):
    """Retrieve respective config context."""
    return CONFIG.get(env)


class Config:
    """Define common config along contexts."""

    DEBUG = False
    TESTING = False

    ACTIVITIES_SCHEMA = os.environ.get('ACTIVITIES_SCHEMA', 'collection_builder')
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        'SQLALCHEMY_DATABASE_URI',
        'postgresql://postgres:postgres@localhost:5432/bdc_catalog?application_name=local-collection'
    )

    # LaSRC/Fmask4 Processor
    LASRC_CONFIG = dict(
        LASRC_DOCKER_IMAGE=os.getenv('LASRC_DOCKER_IMAGE', 'registry.dpi.inpe.br/brazildatacube/lasrc-ledaps-fmask:1.0.2'),
        LASRC_AUX_DIR=os.getenv('LASRC_AUX_DIR', '/data/auxiliaries/lasrc'),
        LEDAPS_AUX_DIR=os.getenv('LEDAPS_AUX_DIR', '/data/auxiliaries/ledaps'),
    )
    # Sen2Cor/Fmask Processor
    SEN2COR_CONFIG = dict(
        SEN2COR_DOCKER_IMAGE=os.getenv('SEN2COR_DOCKER_IMAGE', 'registry.dpi.inpe.br/brazildatacube/sen2cor:2.8.0'),
        SEN2COR_AUX_DIR=os.getenv('SEN2COR_AUX_DIR', '/data/auxiliaries/sen2cor/CCI4SEN2COR'),
        SEN2COR_CONFIG_DIR=os.getenv('SEN2COR_CONFIG_DIR', '/data/auxiliaries/sen2cor/config/2.8')
    )

    # Google Credentials support (Deprecated, use Provider.credentials instead.)
    GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '')

    # Feature to synchronize data with AWS Buckets.
    COLLECTION_BUILDER_SYNC = strtobool(str(os.getenv('COLLECTION_BUILDER_SYNC', False)))
    COLLECTION_BUILDER_SYNC_BUCKET = os.getenv('COLLECTION_BUILDER_SYNC_BUCKET', None)

    # Items - Use AWS_BUCKET_NAME as prefix.
    USE_BUCKET_PREFIX = os.getenv('USE_BUCKET_PREFIX', strtobool(str(os.getenv('USE_BUCKET_PREFIX', False))))

    AWS_BUCKET_NAME = os.environ.get('AWS_BUCKET_NAME', 'bdc-archive')
    AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', 'CHANGE_ME')
    AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', 'CHANGE_ME')
    AWS_REGION_NAME = os.environ.get('AWS_REGION_NAME', 'us-east-1')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')
    RABBIT_MQ_URL = os.environ.get('RABBIT_MQ_URL', 'pyamqp://guest@localhost')

    # The directory where published collections will be stored after collected and processed..
    DATA_DIR = os.environ.get('DATA_DIR', os.path.join(tempfile.gettempdir(), 'collections'))
    # The directory where published data cubes will be stored after collected.
    CUBES_DATA_DIR = os.environ.get('CUBES_DATA_DIR', os.path.join(tempfile.gettempdir(), 'cubes'))
    # The string prefix to be set on the published cube items
    CUBES_ITEM_PREFIX = os.environ.get('CUBES_ITEM_PREFIX', '/data/d006')

    TASK_RETRY_DELAY = int(os.environ.get('TASK_RETRY_DELAY', 60 * 60))  # a hour

    CELERYD_PREFETCH_MULTIPLIER = 1  # disable


class ProductionConfig(Config):
    """Production Mode."""

    DEBUG = False


class DevelopmentConfig(Config):
    """Development Mode."""

    DEBUG = True
    DEVELOPMENT = True


class TestingConfig(Config):
    """Testing Mode (Continous Integration)."""

    TESTING = True
    DEBUG = True


CONFIG = {
    "DevelopmentConfig": DevelopmentConfig(),
    "ProductionConfig": ProductionConfig(),
    "TestingConfig": TestingConfig()
}
