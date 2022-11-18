import getpass
import os
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Sequence,
    String,
    Text,
    between,
    func,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy.orm import relationship

from lightcurvedb import __version__
from lightcurvedb.core.base_model import CreatedOnMixin, QLPModel


class QLPStage(QLPModel, CreatedOnMixin):
    """
    This model encompasses a stage of the QLP pipeline that we wish to
    record for analysis later.
    """

    __tablename__ = "qlpstages"
    id = Column(Integer, Sequence("qlpstage_id_seq"), primary_key=True)
    slug = Column(String(64), unique=True)
    name = Column(String(64), unique=True)
    description = Column(Text(), nullable=True)

    processes = relationship("QLPProcess", backref="stage")


class QLPProcess(QLPModel, CreatedOnMixin):
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

    id = Column(Integer, Sequence("qlpprocess_id_seq"), primary_key=True)
    stage_id = Column(Integer, ForeignKey(QLPStage.id), nullable=False)

    lcdb_version = Column(String(32), index=True, default=__version__)
    process_start = Column(
        DateTime, index=True, nullable=False, server_default=func.now()
    )
    process_completion = Column(DateTime, index=True, nullable=True)

    state = Column(String(64), index=True, default="initialized")
    runtime_parameters = Column(postgresql.JSONB, index=True)
    host = Column(String(64), index=True, default=os.uname().nodename)
    user = Column(String(64), index=True, default=getpass.getuser)

    operations = relationship("QLPOperation", backref="process")

    def __repr__(self):
        return f"QLPProcess: {self.id} {self.lcdb_version} {self.state}"

    def finish(self):
        if self.process_completion is None:
            self.process_completion = datetime.now()
            self.state = "finished"
        raise RuntimeError(
            f"Cannot assign completion date as it already exists for {self}"
        )

    @classmethod
    def current_version(cls):
        from lightcurvedb import __version__

        return cls.lcdb_version == __version__


class QLPOperation(QLPModel, CreatedOnMixin):
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
        Sequence("qlpoperation_id_seq"),
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


class QLPMetricAPIMixin:
    """
    Provide interaction with the previously defined models here to avoid
    making the database connection object too large.
    """

    def get_qlp_stage(self, slug):
        stage = self.query(QLPStage).filter_by(slug=slug).one()
        return stage
