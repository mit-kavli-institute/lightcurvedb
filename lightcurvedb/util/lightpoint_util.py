from sqlalchemy import Table, Column, BigInteger, Integer
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.sql import text
from lightcurvedb.core.connection import db_from_config
from lightcurvedb.core.base_model import QLPModel
from lightcurvedb.models import Lightpoint
import os
from datetime import datetime


def reduce_lp_chunk(chunk):
    """Reduce a enumerated chunk of lightpoint dict
    objects to contain their enumation as the id"""
    for id, lp in chunk:
        lp['id'] = id
        yield lp


def mass_insert_lp(db_config, chunk):
    with db_from_config(db_config) as db:
        chunk_w_id = reduce_lp_chunk(chunk)
        db.session.bulk_insert_mappings(
            Lightpoint,
            chunk_w_id
        )
        db.session.commit()


def mass_upsert_lp(db_config, chunk):
    oid = os.getpid()
    with db_from_config(db_config) as db:
        chunk_w_id = reduce_lp_chunk(chunk)
        q = Lightpoint.bulk_upsert_stmt(chunk_w_id)
        print('{} inserting...'.format(oid))
        db.session.execute(q)
        db.session.commit()
        print('{} done'.format(oid))


def map_existing_lightcurves(db, tics):
    lightcurve_map = {}
    existing_lcs = db.lightcurves_by_tics(tics).all()
    for lc in existing_lcs:
        key = (lc.cadence_type, lc.lightcurve_type_id, lc.aperture_id, lc.tic_id)
        lightcurve_map[key] = lc.id

    return lightcurve_map


def create_lightpoint_tmp_table(basename):
    pid = os.getpid()
    time = str(datetime.now()).replace(':', '_').replace('.','_').replace(' ', '_').replace('-','_')
    name = '{}_{}_{}'.format(basename, pid, time)
    tmp_table = Table(
        name,
        QLPModel.metadata,
        Column('cache_id', BigInteger, primary_key=True),
        Column('lightcurve_id', BigInteger, index=True),
        Column('cadence', Integer, index=True),
        Column('barycentric_julian_date', DOUBLE_PRECISION, index=True),
        Column('value', DOUBLE_PRECISION),
        Column('error', DOUBLE_PRECISION),
        Column('x_centroid', DOUBLE_PRECISION),
        Column('y_centroid', DOUBLE_PRECISION),
        Column('quality_flag', Integer),
        prefixes=['TEMPORARY']
    )
    return tmp_table
