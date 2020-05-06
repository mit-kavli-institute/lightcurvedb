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
