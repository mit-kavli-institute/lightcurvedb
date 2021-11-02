from functools import partial
from multiprocessing import Pool

from sqlalchemy import (
    CheckConstraint,
    Column,
    ForeignKey,
    Integer,
    Sequence,
    String,
    text,
)
from sqlalchemy.ext.hybrid import hybrid_method, hybrid_property
from sqlalchemy.orm import relationship
from tqdm import tqdm

from lightcurvedb.core.base_model import QLPMetric
from lightcurvedb.core.psql_tables import PGClass


class PartitionTrack(QLPMetric):
    __tablename__ = "partition_tracks"

    id = Column(Integer, Sequence("partition_tracks_id_seq"), primary_key=True)
    model = Column(String(64), index=True)
    oid = Column(ForeignKey(PGClass.oid), index=True, unique=True)
    tracker_type = Column(String(64), index=True)

    __mapper_args__ = {
        "polymorphic_identity": "tracks",
        "polymorphic_on": tracker_type,
    }

    pgclass = relationship(PGClass)

    @hybrid_method
    def same_model(self, Model):
        model_str = Model if isinstance(Model, str) else Model.__name__
        return self.model == model_str

    @same_model.expression
    def same_model(cls, Model):
        model_str = Model if isinstance(Model, str) else Model.__name__
        return cls.model == model_str

    @hybrid_method
    def contains_value(self, value):
        return False  # Tautological, no behavior is defined for base class

    @contains_value.expression
    def contains_value(cls, value):
        return False  # Tautological, no behavior is defined for base class

    @hybrid_method
    def in_partition(self, Model, value):
        return self.same_model(Model) and self.contains_value(value)

    @in_partition.expression
    def in_partition(cls, Model, value):
        return cls.same_model(Model) & cls.contains_value(value)

    def get_check_func(self):
        return lambda value: value == self.model

    def generate_detachment_q(self):
        parent = self.pgclass.parent[0]
        target = f"{self.pgclass.namespace.name}.{self.pgclass.name}"
        q = text(
            f"""
            ALTER TABLE {parent.name}
            DETACH PARTITION {target}
            """
        )
        return q

    def generate_attachment_q(self):
        raise NotImplementedError


class RangedPartitionTrack(PartitionTrack):
    __tablename__ = "ranged_partition_tracks"
    id = Column(Integer, ForeignKey(PartitionTrack.id), primary_key=True)
    min_range = Column(Integer, index=True)
    max_range = Column(Integer, index=True)

    __table_args__ = (
        CheckConstraint("max_range > min_range", name="range_validity"),
    )

    __mapper_args__ = {"polymorphic_identity": "ranged"}

    @hybrid_method
    def contains_value(self, value):
        return self.min_range <= value < self.max_range

    @contains_value.expression
    def contains_value(cls, value):
        return (cls.min_range <= value) & (value < cls.max_range)

    def get_check_func(self):
        return self.contains_value

    @hybrid_property
    def length(self):
        return self.max_range - self.min_range

    @length.expression
    def length(cls):
        return cls.max_range - cls.min_range

    def generate_attachment_q(self):
        parent = self.pgclass.parent[0]
        target = f"{self.pgclass.namespace.name}.{self.pgclass.name}"
        q = text(
            f"""
            ALTER TABLE {parent.name}
            ATTACH PARTITION {target}
            FOR VALUES ({self.min_range}) TO ({self.max_range})
            """
        )

        return q


def range_check(ranges, value):
    """
    Search for which range the given value falls under. It is expected that
    the range tuples provided are sorted by the min_value.

    Parameters
    ----------
    ranges: list of sorted tuples of ``(min, max, oid)``
        The ranges to find membership of ``value``.
    value: int
        The ``value`` to find membership for
    Returns
    -------
    (int, int)
        A tuple with element 0 being ``value`` and element 1 being the mapped
        table ``oid``.
    Raises
    ------
    ValueError:
        If the given ``value`` could not be properly mapped.
    """
    left_idx = 0
    right_idx = len(ranges)
    while left_idx != right_idx - 1:
        pivot = left_idx + (right_idx - left_idx) // 2
        min_, max_, oid = ranges[pivot]
        if value < ranges[pivot - 1][1]:
            right_idx = pivot
        elif value >= min_:
            left_idx = pivot
        else:
            raise ValueError(
                "Value {0} falls between range gap!".format(value)
            )
    # Left idx and right idx should be pointing at the same range
    # check if range is valid
    min_, max_, oid = ranges[left_idx]
    if min_ <= value <= max_:
        return value, oid

    # Left and right indexes point to a range which does not match
    raise ValueError("Could not match {0} to any partition".format(value))


class TableTrackerAPIMixin(object):
    def get_partition_map_func(self, Model):
        partition_tracks = self.query(PartitionTrack).filter(
            PartitionTrack.same_model(Model)
        )

        def dynamic_map(value):
            for track in partition_tracks:
                if track.contains_value(value):
                    return track
            raise KeyError(
                "Unable to find Partition Track for "
                "{0} on model {1}".format(value, Model)
            )

        return dynamic_map

    def map_values_to_partitions(self, Model, values, n_workers=None):
        """
        Map given values against a Partitioned model. This function is
        multiprocessed and thus the order of return is not promised to be
        the same as the input.

        Parameters
        ----------
        Model: str or Class
            The model to check the values against. If a Class is passed, the
            name of the class is used.
        values: iterable
            An iterable of values to be compared with Partition Tracks of the
            given Model.
        n_workers: int, optional
            The number of workers to compare values. If set to None, all CPU
            cores will be utilized.
        Yields
        ------
        (object, integer)
            Yields tuples of (value, table OID). The value is of whatever was
            passed as the input sequence. OID is a unique integer describing
            a table relation on the database.

        Raises
        ------
        NotImplementedError:
            Raised if the given Model is not supported for multiprocess
            mapping.
        """
        partition_tracks = list(
            self.query(PartitionTrack).filter(PartitionTrack.same_model(Model))
        )
        if isinstance(partition_tracks[0], RangedPartitionTrack):
            ranges = sorted(
                [(t.min_range, t.max_range, t.oid) for t in partition_tracks],
                key=lambda t: t[0],
            )
            func = partial(range_check, ranges)
        else:
            raise NotImplementedError(
                "No multiprocessing support for tracking type {0}".format(
                    type(partition_tracks[0])
                )
            )

        with Pool(processes=n_workers) as pool:
            for value, oid in tqdm(
                pool.imap_unordered(func, values, chunksize=10000),
                total=len(values),
            ):
                yield value, oid
