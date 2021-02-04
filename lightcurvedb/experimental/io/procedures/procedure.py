import os
from glob import glob
from sqlalchemy import DDL

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
