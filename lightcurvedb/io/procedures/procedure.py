import os
from glob import glob
from sqlalchemy import DDL, column, select, func

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
        'lightcurve_id': column('lightcurve_id'),
        'cadence': column('cadence'),
        'barycentric_julian_date': column('barycentric_julian_date'),
        'data': column('data'),
        'error': column('error'),
        'x_centroid': column('x_centroid'),
        'y_centroid': column('y_centroid'),
        'quality_flag': column('quality_flag'),
}


def get_bestaperture_data(tic_id, *columns):
    """
    Interface for get_bestaperture_data.sql stored procedure.
    """
    if not columns:
        columns = ('lightcurve_id', 'cadence', 'barycentric_julian_date', 'data', 'error', 'x_centroid', 'y_centroid', 'quality_flag')

    lp_cols = (
        LIGHTPOINT_COL_AS_PROC[c] for c in columns
    )

    stmt = (
        select(lp_cols)
        .select_from(
            func.get_bestaperture_data(tic_id).alias()
        )
    )

    return stmt
