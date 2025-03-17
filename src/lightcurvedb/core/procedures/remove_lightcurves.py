import pathlib

import sqlalchemy as sa
from loguru import logger

from lightcurvedb import models as m
from lightcurvedb.core.connection import db_from_config


def delete_lightcurves(
    db_conf: pathlib.Path, orbit_number: int, tic_ids: list[int]
):
    with db_from_config(db_conf) as db:
        orbit = db.scalar(
            sa.select(m.Orbit).where(m.Orbit.orbit_number == orbit_number)
        )
        logger.debug(
            f"Removing Best Orbit Lightcurve entries for {len(tic_ids)} stars"
        )
        del_q = sa.delete(m.BestOrbitLightcurve).where(
            m.BestOrbitLightcurve.orbit_id == orbit.id,
            m.BestOrbitLightcurve.tic_id.in_(tic_ids),
        )
        db.execute(del_q)
        logger.debug(f"Removing Lightcurve entries for {len(tic_ids)} stars")
        lc_del_q = sa.delete(m.ArrayOrbitLightcurve).where(
            m.ArrayOrbitLightcurve.orbit_id == orbit.id,
            m.ArrayOrbitLightcurve.tic_id.in_(tic_ids),
        )
        db.execute(lc_del_q)
        db.commit()
        logger.success(f"Removed data for {len(tic_ids)} stars on {orbit}")
    return len(tic_ids)
