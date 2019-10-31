import logging
import redis
from bdc_scripts.config import Config


client = redis.Redis.from_url(Config.REDIS_URL)


class LockHandler:
    """
    Controls the life cycle of Redis Locks on Celery

    Releases all locks when instance is destroyed
    """
    def __init__(self):
        self._locks = []

    def lock(self, name, **options):
        lock = client.lock(name, **options)

        self._locks.append(lock)

        return lock

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release_all()

    def release_all(self):
        logging.debug('Releasing locks...')
        for lock in self._locks:
            if lock.locked():
                try:
                    lock.release()
                except:
                    logging.debug('Could not release lock!')


lock_handler = LockHandler()