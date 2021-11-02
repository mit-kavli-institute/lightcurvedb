from click.testing import CliRunner

from lightcurvedb.experimental.io.procedures.procedure import PROCEDURE_FILES
from .constants import CONFIG_PATH
from .fixtures import db_conn


def test_procedure_definition(db_conn):

    runner = CliRunner()

    result = runner.invoke(
        lcdbcli,
        [
            "--dbconf",
            CONFIG_PATH,
            "admin",
            "procedures",
            "reload",
        ],
    )

    assert "Success" in result

    result = runner.invoke(
        lcdbcli,
        ["--dbconf", CONFIG_PATH, "admin", "procedures", "list-defined"],
    )

    rows = result.split("\n")

    assert len(rows) >= (len(PROCEDURE_FILES) + 1)
