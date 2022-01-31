from click.testing import CliRunner
from hypothesis import given, note, settings
from hypothesis import strategies as st

from lightcurvedb.cli.base import lcdbcli

from .constants import CONFIG_PATH
from .factories import orbit
from .fixtures import clear_all, db_conn  # noqa F401


@settings(deadline=None, max_examples=10)
@given(
    st.lists(
        orbit(),
        unique_by=(lambda o: o.orbit_number, lambda o: o.basename),
        min_size=1,
        max_size=3,
    )
)
def test_query_on_orbits(db_conn, orbits):  # noqa F401
    runner = CliRunner()

    for ith, orbit_obj in enumerate(orbits):
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


@settings(deadline=None, max_examples=10)
@given(
    st.lists(
        orbit(),
        unique_by=(lambda o: o.orbit_number, lambda o: o.basename),
        min_size=1,
        max_size=3,
    )
)
def test_query_on_orbits_w_filter(db_conn, orbits):  # noqa F401
    runner = CliRunner()

    for ith, orbit_obj in enumerate(orbits):
        orbit_obj.id = ith

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
                    "-p",
                    "orbit_number",
                    "-f",
                    "id = {0}".format(orbits[0].id),
                ],
                catch_exceptions=False,
            )
            note("output: {0}".format(result.output))

            assert str(orbits[0].orbit_number) in result.output

            for orbit_obj in orbits[1:]:
                check = " {0} ".format(orbit_obj.orbit_number)
                assert check not in result.output

        finally:
            db.rollback()
            clear_all(db)
