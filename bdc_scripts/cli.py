"""
Brazil Data Cube Scripts

Creates a python click context and inject it to the global flask commands

It allows to call own
"""


import click
from flask.cli import FlaskGroup
from bdc_scripts import create_app


def create_cli(create_app=None):
    def create_cli_app(info):
        if create_app is None:
            info.create_app = None

            app = info.load_app()
        else:
            app = create_app()

        return app

    @click.group(cls=FlaskGroup, create_app=create_cli_app)
    def cli(**params):
        """Command line interface for bdc-scripts"""
        pass

    return cli


cli = create_cli(create_app=create_app)
