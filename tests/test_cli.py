import traceback

from click.testing import CliRunner
from hypothesis import given, note, settings
from hypothesis import strategies as st

from lightcurvedb import Orbit
from lightcurvedb.cli.base import lcdbcli

from .constants import CONFIG_PATH
from .factories import orbit
from .fixtures import clear_all, db_conn


@settings(deadline=None)
@given(
    st.lists(
        orbit(), unique_by=(lambda o: o.orbit_number, lambda o: o.basename)
    )
)
def test_query_on_orbits(db_conn, orbits):
    runner = CliRunner()

    for ith, orbit in enumerate(orbits):
        orbit.id = ith

    with db_conn as db:
        try:
            db.session.add_all(orbits)
            db.commit()

            result = runner.invoke(
                lcdbcli,
                [
                    "--dbconf",
                    CONFIG_PATH,
                    "query",
                    "Orbit",
                    "print-table",
                    "-p" "orbit_number",
                ],
                catch_exceptions=False,
            )
            note("output: {0}".format(result.output))
            assert all(str(o.orbit_number) in result.output for o in orbits)
        finally:
            db.rollback()
            clear_all(db)
