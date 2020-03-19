from hypothesis import strategies as st
from lightcurvedb.core.connection import db_from_config

from .constants import CONFIG_PATH

def test_connection_spawning():
    db = db_from_config(CONFIG_PATH)

    # Assert base connections are uninitialized
    assert not db.is_active

def test_connection_enter_context():
    db = db_from_config(CONFIG_PATH)

    with db as x:
        assert db.session is not None
        assert db.is_active

    assert not db.is_active
