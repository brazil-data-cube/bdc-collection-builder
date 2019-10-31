import os


CURRENT_DIR = os.path.dirname(__file__)


def get_settings(env):
    return CONFIG.get(env)


class Config:
    DEBUG = False
    TESTING = False
    REDIS_URL = 'redis://localhost:6380'


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
