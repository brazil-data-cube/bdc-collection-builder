#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Unit-test for BDC-Collection-Builder CLI."""

import subprocess
import sys

from bdc_catalog.cli import cli
from click.testing import CliRunner


def test_basic_cli():
    """Test basic cli usage."""
    res = CliRunner().invoke(cli)

    assert res.exit_code == 0


def test_cli_module():
    """Test the package BDC-Collection-Builder invoked as a module."""
    res = subprocess.call(f'{sys.executable} -m bdc_catalog', shell=True)

    assert res == 0
