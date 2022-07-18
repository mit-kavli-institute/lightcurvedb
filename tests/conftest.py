import pathlib
import os
import tempfile

import pytest
from click.testing import CliRunner


@pytest.fixture(scope="module")
def tempdir():
    with tempfile.TemporaryDirectory() as tmp:
        yield pathlib.Path(tmp)


@pytest.fixture(scope="module")
def clirunner():
    runner = CliRunner(mix_stderr=True)

    return runner
