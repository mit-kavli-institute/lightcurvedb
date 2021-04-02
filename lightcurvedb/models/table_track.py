from lightcurvedb.core.base_model import QLPMetric

from sqlalchemy import (
    Integer,
    String,
    CheckConstraint,
    Column, Sequence,
    ForeignKey
)
from sqlalchemy.ext.hybrid import hybrid_method


class PartitionTrack(QLPMetric):
    __tablename__ = "partition_tracks"

    id = Column(Integer, Sequence("partition_tracks_id_seq"), primary_key=True)
    model = Column(String(64), index=True)
    oid = Column(Integer, index=True, unique=True)
    tracker_type = Column(String(64), index=True)

    __mapper_args__ = {
        "polymorphic_identity": "tracks",
        "polymorphic_on": tracker_type
    }

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


class RangedPartitionTrack(PartitionTrack):
    __tablename__ = "ranged_partition_tracks"
    id = Column(Integer, ForeignKey(PartitionTrack.id), primary_key=True)
    min_range = Column(Integer, index=True)
    max_range = Column(Integer, index=True)

    __table_args__ = (
        CheckConstraint(
            "max_range > min_range",
            name="range_validity"
        ),
    )

    __mapper_args__ = {
        "polymorphic_identity": "ranged"
    }

    @hybrid_method
    def contains_value(self, value):
        return self.min_range <= value < self.max_range

    @contains_value.expression
    def contains_value(cls, value):
        return (cls.min_range <= value) & (value < cls.max_range)

    def get_check_func(self):
        return self.contains_value
    

class TableTrackerAPIMixin(object):
    def get_partition_map_func(self, Model):
        partition_tracks = self.query(PartitionTrack).filter(PartitionTrack.same_model(Model))
        def dynamic_map(value):
            for track in partition_tracks:
                if track.contains_value(value):
                    return track
            raise KeyError(
                "Unable to find Partition Track for "
                "{0} on model {1}".format(value, Model)
            )
        return dynamic_map
