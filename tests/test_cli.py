from click.testing import CliRunner
from hypothesis import strategies as st, given
from lightcurvedb import Orbit
from .fixtures import db_conn, clear_all
from .factories import orbit
from lightcurvedb.cli.base import lcdbcli


@given(
    st.lists(
        orbit(),
        unique_by=(lambda o: (o.orbit_number, o.basename))
    )
)
def test_query_on_orbits(db_conn, orbits):
    runner = CliRunner()

    with db_conn as db:
        try:
            db.session.add_all(orbits)
            db.commit()

            result = runner.invoke(
                lcdbcli,
                'query',
                'Orbit',
                '-p orbit_number'
            )
            note("CLI OUT: {0}".format(result))
            assert all(o.orbit_number in result for o in orbits)

        finally:
            db.rollback()
            clear_all(db)
