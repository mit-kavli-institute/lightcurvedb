from lightcurvedb import __version__
from lightcurvedb.core.base_model import QLPMetric
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Sequence,
    SmallInteger,
    String,
    Text,
    between,
    func
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.hybrid import hybrid_method, hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.orm.query import Query
from datetime import datetime


class QLPStage(QLPMetric):
    """
    This model encompasses a stage of the QLP pipeline that we wish to
    record for analysis later.
    """
    __tablename__ = "qlpstages"
    id = Column(Integer, Sequence("qlpstage_id_seq"), primary_key=True)
    name = Column(String(64), unique=True)
    slug = Column(String(64), unique=True)
    description = Column(Text)

    processes = relationship("QLPProcess", backref="stage")


class QLPProcess(QLPMetric):
    """
    This model is used to describe various processes interacting on the
    database. The job type, description and use is up to the user to
    define and maintain.

    Attributes
    ----------
    id : int
        The primary key of the model. Do not edit unless you are confident
        in the repercussions.
    job_type : str
        The name of the job performed.
    job_version_major : int
        The major versioning number of the job performed. If a project has the
        version string ``1.2.04`` then the major version number is ``1``.
    job_version_minor : int
        The minor versioning number of the job performed. If a project has the
        version string ``1.2.04`` then the minor version number is ``2``.
    job_version_revision : int
        The revision number of the job performed. If a project has the version
        string ``1.2.04`` then the revision number is ``4``. Keep this in mind
        with zero padded versionings.
    job_description : str
        The description for this process.
    additional_version_info : dict
        A JSON stored datastructure which contains all major, minor versions
        along with other version info which does not fit into the scalar
        version descriptions on the base model.
    version : packaging.version.Version
        Return a Version object.
    """

    __tablename__ = "qlpprocesses"

    id = Column(Integer, Sequence("qlpalertation_id_seq"), primary_key=True)
    stage_id = Column(Integer, ForeignKey(QLPStage.id), nullable=False)

    lcdb_version = Column(String(32), index=True, default=__version__)
    process_start = Column(DateTime, index=True, nullable=False, server_default=func.now())
    process_completion = Column(DateTime, index=True, nullable=True)

    state = Column(String(64), index=True, default="initialized")
    runtime_parameters = Column(postgresql.JSONB, index=True)
    host = Column(String(64), index=True)
    user = Column(String(64), index=True)

    operations = relationship("QLPOperations", backref="process")

    def finish(self):
        if self.process_completion is None:
            self.process_completion = datetime.now()
            self.state = "finished"
        raise RuntimeError(
            f"Cannot assign completion date as it already exists for {self}"
        )


class QLPOperation(QLPMetric):
    """
    This model is used to describe various alterations performed on. This
    is best used to describe atomic operations, like single insert,
    update, delete commands. But QLPAlteration may also be used to define
    other non-database operations. The parameters set are up to the user/
    developer to interperet and keep consistent.
    """

    __tablename__ = "qlpoperations"
    id = Column(
        BigInteger,
        Sequence("qlpwork_id_seq"),
        primary_key=True,
    )
    process_id = Column(Integer, ForeignKey(QLPProcess.id), nullable=False)

    job_size = Column(BigInteger, nullable=False)
    unit = Column(String(32), nullable=False, default="unit")
    time_start = Column(DateTime(), nullable=False, index=True)
    time_end = Column(DateTime(), nullable=False, index=True)

    @hybrid_method
    def date_during_job(self, target_date):
        return self.time_start < target_date < self.time_end

    @date_during_job.expression
    def date_during_job(cls, target_date):
        return between(target_date, cls.time_start, cls.time_end)
