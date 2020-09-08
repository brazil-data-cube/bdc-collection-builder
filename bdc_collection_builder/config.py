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
    ENABLE_REFRESH_VIEW = strtobool(str(os.environ.get('ENABLE_REFRESH_VIEW', False)))

    ACTIVITIES_SCHEMA = os.environ.get('ACTIVITIES_SCHEMA', 'collection_builder')
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        'SQLALCHEMY_DATABASE_URI',
        'postgresql://postgres:postgres@localhost:5432/bdc_catalog?application_name=local-collection'
    )
    SQLALCHEMY_DATABASE_URI_AWS = os.environ.get(
        'SQLALCHEMY_DATABASE_URI_AWS',
        'postgresql://postgres:postgres@localhost:5432/bdc_catalog_aws?application_name=local-collection'
    )
    ITEM_ASSET_PREFIX = os.getenv('ITEM_ASSET_PREFIX', '/Repository/Archive')
    DISABLE_PUBLISH_SECOND_DB = strtobool(str(os.getenv('DISABLE_PUBLISH_SECOND_DB', True)))
    GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '')

    # Feature to synchronize data with AWS Buckets.
    COLLECTION_BUILDER_SYNC = strtobool(str(os.getenv('COLLECTION_BUILDER_SYNC', False)))
    COLLECTION_BUILDER_SYNC_BUCKET = os.getenv('COLLECTION_BUILDER_SYNC_BUCKET', None)

    AWS_BUCKET_NAME = os.environ.get('AWS_BUCKET_NAME', 'bdc-archive')
    AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', 'CHANGE_ME')
    AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', 'CHANGE_ME')
    AWS_REGION_NAME = os.environ.get('AWS_REGION_NAME', 'us-east-1')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')
    RABBIT_MQ_URL = os.environ.get('RABBIT_MQ_URL', 'pyamqp://guest@localhost')
    DATA_DIR = os.environ.get('DATA_DIR', tempfile.gettempdir())
    AUXILIARIES_DIR = os.environ.get('AUXILIARIES_DIR', os.path.join(DATA_DIR, 'ds_data'))
    ESPA_URL = os.environ.get('ESPA_URL', 'http://127.0.0.1:5032')
    SEN2COR_URL = os.environ.get('SEN2COR_URL', 'http://127.0.0.1:5031')
    CLIENT_SECRET_KEY = os.environ.get('CLIENT_SECRET_KEY', 'CHANGE_ME')
    CLIENT_AUDIENCE = os.environ.get('CLIENT_AUDIENCE', 'CHANGE_ME')
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
