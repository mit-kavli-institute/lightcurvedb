import os
from time import time

from loguru import logger
from sqlalchemy import text

from lightcurvedb.core.psql_tables import PGClass
from lightcurvedb.io.pipeline import db_scope
from lightcurvedb.models.table_track import RangedPartitionTrack


# Begin merging definitions
class WorkingPair(object):
    def __init__(self, left_partition_oid, right_partition_oid):
        self.left = left_partition_oid
        self.right = right_partition_oid

    @property
    def eligible(self):
        return self.left is not None and self.right is not None

    def get_pgclasses(self, db):
        left_class = db.query(PGClass).get(self.left)
        right_class = db.query(PGClass).get(self.right)

        return left_class, right_class

    def get_tracks(self, db):
        q = db.query(RangedPartitionTrack)
        left = q.filter_by(oid=self.left).one()
        right = q.filter_by(oid=self.right).one()

        return left, right


def detach(db, class_):

    namespace = class_.namespace.name
    tablename = class_.name

    parent_tablename = class_.parent[0].name

    q = text(
        f"ALTER TABLE {parent_tablename} "
        f"DETACH PARTITION {namespace}.{tablename}"
    )

    db.execute(q)
    logger.info(f"Detached {tablename}")


def pull_over_to(db, dest, src):
    q = text(
        f"INSERT INTO {src.namespace.name}.{src.name} "
        f"(SELECT * FROM {dest.namespace.name}.{dest.name} "
        "ORDER BY lightcurve_id, cadence)"
    )
    logger.info(f"Pulling data from {src.name} to {dest.name}")
    t0 = time()
    db.execute(q)
    elapsed = time() - t0

    logger.debug(f"Pulled {src.name} which took {elapsed} seconds")


def update_tracks(db, dest, source):
    minimum_ranges = [dest.min_range, source.min_range]
    maximum_ranges = [dest.max_range, source.max_range]

    dest.min_range = min(minimum_ranges)
    dest.max_range = max(maximum_ranges)


def update_pgclass(db, parent, class_, track):
    namespace = class_.namespace.name
    tablename = class_.name

    new_name = f"{parent}_{track.min_range}_{track.max_range}"

    q = text(f"ALTER TABLE {namespace}.{tablename} RENAME TO {new_name}")
    db.execute(q)

    logger.info(f"Renamed {tablename} to {new_name}")


def remove_old_tracks(db, class_, track):
    namespace = class_.namespace.name
    tablename = class_.name

    q = text(f"DROP TABLE {namespace}.{tablename}")
    db.delete(track)
    db.execute(q)

    logger.info(f"Removed {tablename} and it's track")


def attach(db, parent, class_, track):
    namespace = class_.namespace.name
    tablename = class_.name

    q = text(
        f"ALTER TABLE {parent} "
        f"ATTACH PARTITION {namespace}.{tablename} "
        f"FOR VALUES FROM ({track.min_range}) TO ({track.max_range})"
    )
    db.execute(q)


@db_scope()
def merge_working_pair(db, working_pair):
    if not working_pair.eligible:
        raise RuntimeError

    try:
        left_class, right_class = working_pair.get_pgclasses(db)
        left_track, right_track = working_pair.get_tracks(db)

        if len(left_class.parent) == 0:
            logger.warning(f"{left_class} already detached...")
            parent = "lightpoints"
        else:
            parent = left_class.parent[0].name
            detach(db, left_class)
            db.commit()

        if len(right_class.parent) == 0:
            logger.warning(f"{right_class} already detached...")
        else:
            parent = right_class.parent[0].name
            detach(db, right_class)
            db.commit()
    except Exception:
        logger.exception("Cowardly not committing detachment!")
        db.rollback()
        return None

    try:
        pull_over_to(db, left_class, right_class)
        update_tracks(db, left_track, right_track)
        update_pgclass(db, parent, left_class, left_track)
        remove_old_tracks(db, right_class, right_track)
        attach(db, parent, left_class, left_track)

        # Finalize commit
        db.commit()

        logger.info(f"Finished {left_class.name}")
        return left_track.oid
    except Exception:
        logger.exception(
            f"Could not process {left_class.name} and {right_class.name}"
        )
        logger.error(
            f"OIDS NOT PROPERLY ATTACHED: {left_track.oid}, {right_track.oid}"
        )
        db.rollback()
        pid = os.getpid()
        with open(f"{pid}-bad-merge.err", "at") as fout:
            fout.write(f"{left_track.oid} {right_track.oid}\n")
        return left_track.oid, right_track.oid
