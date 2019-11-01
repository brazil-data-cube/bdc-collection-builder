import os
import tempfile


CURRENT_DIR = os.path.dirname(__file__)


def get_settings(env):
    return CONFIG.get(env)


class Config:
    DEBUG = False
    TESTING = False
    REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6380')
    RABBIT_MQ_URL = os.environ.get('RABBIT_MQ_URL', 'pyamqp://guest@localhost')
    DATA_DIR = os.environ.get('DATA_DIR', tempfile.gettempdir())


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
