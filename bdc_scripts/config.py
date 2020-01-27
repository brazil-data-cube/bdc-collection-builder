import os
import tempfile


CURRENT_DIR = os.path.dirname(__file__)


def get_settings(env):
    return CONFIG.get(env)


class Config:
    DEBUG = False
    TESTING = False
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        'SQLALCHEMY_DATABASE_URI',
        'postgresql://postgres:postgres@localhost:5433/bdc_scripts'
    )
    SQLALCHEMY_DATABASE_URI_AWS = os.environ.get(
        'SQLALCHEMY_DATABASE_URI_AWS',
        'postgresql://postgres:postgres@localhost:5433/bdc_scripts'
    )
    AWS_BUCKET_NAME = os.environ.get('AWS_BUCKET_NAME', 'bdc-arquive')
    AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', 'CHANGE_ME')
    AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', 'CHANGE_ME')
    AWS_REGION_NAME = os.environ.get('AWS_REGION_NAME', 'us-east-1')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')
    RABBIT_MQ_URL = os.environ.get('RABBIT_MQ_URL', 'pyamqp://guest@localhost')
    DATA_DIR = os.environ.get('DATA_DIR', tempfile.gettempdir())
    ESPA_URL = os.environ.get('ESPA_URL', 'http://127.0.0.1:5032')
    SEN2COR_URL = os.environ.get('SEN2COR_URL', 'http://127.0.0.1:5031')
    STAC_URL = os.environ.get('STAC_URL', 'http://brazildatacube.dpi.inpe.br/bdc-stac/0.7.0/')
    CLIENT_SECRET_KEY = os.environ.get('CLIENT_SECRET_KEY', 'CHANGE_ME')
    CLIENT_AUDIENCE = os.environ.get('CLIENT_AUDIENCE', 'CHANGE_ME')


class ProductionConfig(Config):
    """Production Mode"""
    DEBUG = False


class DevelopmentConfig(Config):
    """Development Mode"""
    DEVELOPMENT = True


class TestingConfig(Config):
    """Testing Mode (Continous Integration)"""
    TESTING = True
    DEBUG = True


CONFIG = {
    "DevelopmentConfig": DevelopmentConfig(),
    "ProductionConfig": ProductionConfig(),
    "TestingConfig": TestingConfig()
}
