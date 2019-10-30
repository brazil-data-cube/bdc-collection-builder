import json
import os
from bdc_scripts.celery import app

print(app.pool)


class UserClients:
    def __init__(self):
        self._users = []
        self._load()

    def _load(self):
        file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'secrets_s2.json')

        if not os.path.exists(file):
            raise FileNotFoundError('The file "{}" does not exists'.format(file))

        fh = open(file, 'r')
        self.users = json.load(fh)

    def list_busy_users(self):
        return


sentinel_clients = UserClients()