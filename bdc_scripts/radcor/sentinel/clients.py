import json
import logging
import os
from bdc_scripts.celery.cache import client
from bdc_scripts.config import CURRENT_DIR


class AtomicUser:
    """
    An abstraction of Atomic User. You must use it as context manager. See contextlib.

    Make sure to control the access to the shared resource.

    Whenever an instance object out of scope, it automatically releases the user to the
    Redis cache.

    Example:
        >>> from bdc_scripts.celery.cache import client
        >>> from bdc_scripts.radcor.sentinel.clients import sentinel_clients
        >>>
        >>> # Lock the access to the shared resource
        >>> with client.lock('my_lock'):
        >>>     user = None
        >>>     while user is None:
        >>>         user = sentinel_clients.use()
        >>>
        >>>     with user:
        >>>         # Do things, download images...
        >>>         pass
        >>>     # User is released on redis
        >>> # Lock released
    """
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self._released = False

    def __repr__(self):
        return 'AtomicUser({}, released={})'.format(self.username, self._released)

    def __enter__(self):
        return self

    def __del__(self):
        self.release()

    def release(self):
        """Release atomic user from redis"""
        if not self._released:
            logging.debug('Release {}'.format(self.username))
            sentinel_clients.done(self.username)

            self._released = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context. Release the user from redis client"""
        self.release()


class UserClients:
    def __init__(self):
        self._users = []
        self._key = 'bdc_scripts:users'
        self._load_from_disk()

    def _load_from_disk(self):
        file = os.path.join(os.path.dirname(CURRENT_DIR), 'secrets.json')

        if not os.path.exists(file):
            raise FileNotFoundError('The file "{}" does not exists'.format(file))

        content = open(file, 'r')

        data = json.loads(content.read())

        assert 'sentinel' in data

        self.users = data['sentinel']

    @property
    def users(self):
        return json.loads(client.get(self._key))

    @users.setter
    def users(self, obj):
        client.set(self._key, json.dumps(obj))

    def use(self):
        users = self.users

        for username, value in users.items():
            if value['count'] < 2:

                logging.debug('User {} - {}'.format(username, value['count']))
                value['count'] += 1

                self.users = users

                return AtomicUser(username, value['password'])
        return None

    def done(self, username):
        users = self.users
        assert username in users.keys()

        users[username]['count'] -= 1

        self.users = users


sentinel_clients = UserClients()