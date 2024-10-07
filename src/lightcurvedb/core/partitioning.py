"""
This module describes partitioning of the lightcurve database.
"""
import sqlalchemy as sa
from loguru import logger

from lightcurvedb.core.connection import db_from_config


def reorder_chunk(db_config, chunkpath, index):
    reorder_q = sa.select().select_from(
        sa.func.reorder_chunk(chunkpath, index)
    )
    logger.debug(f"Clustering {chunkpath}")
    with db_from_config(db_config, isolation_level="AUTOCOMMIT") as db:
        db.execute(reorder_q)
    logger.debug(f"Finished clustering {chunkpath}")
    return chunkpath
