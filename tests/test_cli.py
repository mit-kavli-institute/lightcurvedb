import traceback

from click.testing import CliRunner
from hypothesis import given, note, settings
from hypothesis import strategies as st

from lightcurvedb import Orbit
from lightcurvedb.core.base_model import QLPDataSubType
from lightcurvedb.cli.base import lcdbcli

from .constants import CONFIG_PATH
from .factories import orbit
from .fixtures import clear_all, db_conn
from .strategies import postgres_text


@settings(deadline=None)
@given(
    st.sampled_from([cls.__name__ for cls in QLPDataSubType.__subclasses__()]),
    postgres_text(min_size=1, max_size=32),
    st.one_of(st.none(), postgres_text()),
    st.one_of(st.none(), st.datetimes())
)
def test_add_qlp_type(db_conn, modelname, name, description, created_on):
    runner = CliRunner()

    cols = [name, description, created_on]

    inputstr = "\n".join(map(lambda c: str(c) if c else '', cols))
    Model = QLPDataSubType.get_model(modelname)

    with db_conn as db:
        try:
            result = runner.invoke(
                lcdbcli,
                [
                    "--dbconf",
                    CONFIG_PATH,
                    "data-types",
                    "add-type",
                    modelname
                ],
                input=inputstr
            )

            note(result.output)
            note(traceback.print_exception(*tuple(result.exc_info)))
            assert not result.exception
            assert db.query(Model).filter(
                Model.name == name
            ).count() == 1

        finally:
            db.rollback()
            clear_all(db)


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


@settings(deadline=None)
@given(
    st.lists(
        orbit(), unique_by=(lambda o: o.orbit_number, lambda o: o.basename),
        min_size=1,
        max_size=10
    )
)
def test_query_on_orbits_w_filter(db_conn, orbits):
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
                    "-p", "orbit_number",
                    "-f", "id = {0}".format(orbits[0].id)
                ],
                catch_exceptions=False,
            )
            note("output: {0}".format(result.output))

            assert str(orbits[0].orbit_number) in result.output

            for orbit in orbits[1:]:
                check = " {0} ".format(orbit.orbit_number)
                assert check not in result.output

        finally:
            db.rollback()
            clear_all(db)
