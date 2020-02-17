#
# This file is part of BDC Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# BDC Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Describe Abstraction for Sentinel Data Access on Copernicus."""

import json
import logging
import os
from bdc_collection_builder.celery.cache import client
from bdc_collection_builder.config import CURRENT_DIR


class AtomicUser:
    """
    An abstraction of Atomic User. You must use it as context manager. See contextlib.

    Make sure to control the access to the shared resource.

    Whenever an instance object out of scope, it automatically releases the user to the
    Redis cache.

    Example:
        >>> from bdc_collection_builder.celery.cache import client
        >>> from bdc_collection_builder.collections.sentinel.clients import sentinel_clients
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
        """Build an atomic user."""
        self.username = username
        self.password = password
        self._released = False

    def __repr__(self):
        """Retrieve string representation of Atomic User."""
        return 'AtomicUser({}, released={})'.format(self.username, self._released)

    def __enter__(self):
        """Open atomic user context."""
        return self

    def __del__(self):
        """Release atomic user from copernicus."""
        self.release()

    def release(self):
        """Release atomic user from redis."""
        if not self._released:
            logging.debug('Release {}'.format(self.username))
            sentinel_clients.done(self.username)

            self._released = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context. Release the user from redis client."""
        self.release()


class UserClients:
    """Global user client for Sentinel Accounts."""

    def __init__(self):
        """Build user clients interface."""
        self._users = []
        self._key = 'bdc_collection_builder:users'
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
        """Retrieve all users from disk."""
        return json.loads(client.get(self._key))

    @users.setter
    def users(self, obj):
        """Update users."""
        client.set(self._key, json.dumps(obj))

    def use(self):
        """Try to lock an atomic user."""
        users = self.users

        for username, value in users.items():
            if value['count'] < 2:

                logging.debug('User {} - {}'.format(username, value['count']))
                value['count'] += 1

                self.users = users

                return AtomicUser(username, value['password'])
        return None

    def done(self, username):
        """Release atomic user."""
        users = self.users
        assert username in users.keys()

        users[username]['count'] -= 1

        self.users = users


sentinel_clients = UserClients()