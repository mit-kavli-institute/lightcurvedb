from sqlalchemy import Table, Column, BigInteger, Integer
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.sql import text
from lightcurvedb.core.connection import db_from_config
from lightcurvedb.core.base_model import QLPModel
from lightcurvedb.models import Lightpoint
import os
from datetime import datetime


def map_existing_lightcurves(db, tics):
    lightcurve_map = {}
    existing_lcs = db.lightcurves_by_tics(tics).all()
    for lc in existing_lcs:
        key = (lc.cadence_type, lc.lightcurve_type_id, lc.aperture_id, lc.tic_id)
        lightcurve_map[key] = lc.id

    return lightcurve_map


def create_lightpoint_tmp_table(basename, metadata):
    pid = os.getpid()
    time = str(datetime.now()).replace(':', '_').replace('.','_').replace(' ', '_').replace('-','_')
    name = '{}_{}'.format(basename, pid)
    tmp_table = Table(
        name,
        metadata,
        Column('cache_id', BigInteger, primary_key=True),
        Column('lightcurve_id', BigInteger, index=True),
        Column('cadence', Integer, index=True),
        Column('barycentric_julian_date', DOUBLE_PRECISION, index=True),
        Column('value', DOUBLE_PRECISION),
        Column('error', DOUBLE_PRECISION),
        Column('x_centroid', DOUBLE_PRECISION),
        Column('y_centroid', DOUBLE_PRECISION),
        Column('quality_flag', Integer),
    )
    return tmp_table
