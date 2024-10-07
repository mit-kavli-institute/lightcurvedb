import os
from glob import glob

from sqlalchemy import DDL, column, func, select

PROCEDURE_BASE = os.path.dirname(os.path.realpath(__file__))
PROCEDURE_DEF_DIR = os.path.join(PROCEDURE_BASE)
PROCEDURE_FILES = glob(os.path.join(PROCEDURE_DEF_DIR, "*.sql"))


def _yield_procedure_ddl():
    """
    Yield DDL statements with source SQL text defined in files located
    in "lightcurvedb.experimental.procedures.
    """
    for filepath in PROCEDURE_FILES:
        procedure_string = open(filepath, "rt").read()
        yield DDL(procedure_string)


LIGHTPOINT_COL_AS_PROC = {
    "lightcurve_id": column("lightcurve_id"),
    "cadence": column("cadence"),
    "barycentric_julian_date": column("barycentric_julian_date"),
    "data": column("data"),
    "error": column("error"),
    "x_centroid": column("x_centroid"),
    "y_centroid": column("y_centroid"),
    "quality_flag": column("quality_flag"),
}


def get_bestaperture_data(tic_id, *columns):
    """
    Interface for get_bestaperture_data.sql stored procedure.
    """

    lp_cols = (LIGHTPOINT_COL_AS_PROC[c] for c in columns)

    stmt = select(lp_cols).select_from(
        func.get_bestaperture_data(tic_id).alias()
    )

    return stmt


def get_lightcurve_data(lightcurve_id, *columns):
    """
    Interface for get_lightcurve_data_by_id stored procedure
    """
    lp_cols = (LIGHTPOINT_COL_AS_PROC[c] for c in columns)

    stmt = select(lp_cols).select_from(
        func.get_lightcurve_data_by_id(lightcurve_id).alias()
    )

    return stmt
