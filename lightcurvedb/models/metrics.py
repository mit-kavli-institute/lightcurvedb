from lightcurvedb.core.base_model import QLPMetric
from sqlalchemy import (BigInteger, Column, DateTime, Float, ForeignKey, Index,
                        Integer, Sequence, SmallInteger, String, Text)
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm.query import Query
from sqlalchemy.ext.hybrid import hybrid_property
from importlib import import_module  # Say that 5 times
from packaging.version import Version, parse


SUPPORTED_VERSION_ARGS = (
    'public',
    'base_version',
    'epoch',
    'release',
    'major',
    'minor',
    'micro',
    'local',
    'is_prerelease',
    'is_devrelease',
    'is_postrelease'
)


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
    """
    __tablename__ = 'qlpprocesses'
    id = Column(Integer, Sequence('qlpprocess_id_seq'), primary_key=True)
    job_type = Column(String(255), index=True)
    job_version_major = Column(SmallInteger, index=True, nullable=False)
    job_version_minor = Column(SmallInteger, index=True, nullable=False)
    job_version_revision = Column(Integer, index=True, nullable=False)

    additional_version_info = Column(
        postgresql.JSONB,
        index=True,
        nullable=True
    )

    job_description = Text()

    @hybrid_property
    def version(self):

        try:
            return parse(
                self.additional_version_info['base_version']
            )
        except KeyError:
            return parse('.'.join((
                str(self.job_version_major),
                str(self.job_version_minor),
                str(self.job_version_revision)
            )))

    @version.setter
    def version(self, value):
        if isinstance(value, Version):
            self.version = value
        else:
            v = parse(value)

            self.job_version_major = v.major
            self.job_version_minor = v.minor
            self.job_version_revision = v.micro

            self.additional_version_info = {
                k: getattr(v, k) for k in SUPPORTED_VERSION_ARGS
            }

    @classmethod
    def make_from_module(cls, module, job_type, description=None):
        """
        Attempt to create a QLPProcess using an imported module. A string
        may also be passed to be interpreted as an import.

        Arguments
        ---------
        module : obj or str
            The module object or string import path to be used as context.
            The specified module must have a ``__version__`` or ``version``
            attribute.
        job_type : str
            The name of the job.
        description : str, optional
            The description of the job
        Raises
        ------
        AttributeError:
            Raised if no ``__version__`` or ``version`` attributes could be
            found on the module.
        """
        if isinstance(module, str):
            __module = import_module(module)
        else:
            __module = module

        try:
            version = getattr(__module, 'version')
        except AttributeError:
            # Attempt to fallback
            version = getattr(__module, '__version__')

        inst = cls(
            job_type=job_type,
            job_description=description
        )
        inst.version = version

        return inst

    @classmethod
    def lightcurvedb_process(cls, job_type, description=None):
        """
        Factory method of creating a QLPProcess instance using the current
        defined lightcurvedb version number.

        Arguments
        ---------
        job_tpye : str
            The name of the job.
        description : str, optional
            The description of this process.
        """
        return cls.make_from_module(
            'lightcurvedb',
            job_type,
            description=description
        )


class QLPAlteration(QLPMetric):
    """
    This model is used to describe various alertations performed on
    the database. This model is best used to describe atomic operations,
    like single insert, update, delete commands. These commands may interact
    with multiple rows.

    Attributes
    ----------
    id : int
        The primary key of the model. Do not edit unless you are confident
        in the repercussions.
    process_id : int
        The foreign key to the QLPProcess this model belongs to. Do not
        edit manually unless you are confident in the repercussions. You
        should instead assign by ``qlpalteration_instance.process = x``.
    target_model : str
        The full module path to the Model this alteration is on. For example
        a QLPAlteration on Lightcurves will have
        ``lightcurvedb.models.Lightcurve``.
    alteration_type : str
        The alteration type of the alteration. Generally should be `insert` or
        `update`. On python-side this is enforced to be lower case and not
        have any leading formatting characters or whitespace.
    n_altered_rows : int
        The number of rows being altered. With ``update`` this metric will be
        an estimation. PostgreSQL's update commands will return the number
        of rows returned by a WHERE clause. But this might not reflect the
        number of rows actually affected during the update.
    est_row_size : int
        The general `size` of each row. The interpretation is up to the user
        to define. It might be the number of columns affected but with
        models containing ARRAYs it might be the total number of elements.
    time_start : datetime
        The UTC start date of the job. Must be earlier than the time end
        date. This is enforced on a PSQL level.
    time_end : datetime
        The UTC end date of the job.
    query : str
        The query executed by this job. It may not be assigned due to size
        constraints or simplicity (no need to store this query if it's
        just a simple selection)
    """
    __tablename__ = 'qlpalterations'
    id = Column(
        BigInteger,
        Sequence('qlpalteration_id_seq', cache=1000),
        primary_key=True)
    process_id = Column(
        ForeignKey(QLPProcess.id),
        nullable=False
    )

    target_model = Column(
        String(255),
        index=Index(
            'alteration_model',
            'target_model',
            postgresql_using='gin'
        ),
        nullable=False
    )

    _alteration_type = Column(String(255), index=True, nullable=False)
    n_altered_rows = Column(BigInteger, index=True, nullable=False)
    est_row_size = Column(BigInteger, index=True, nullable=False)
    time_start = Column(DateTime(timezone=False), nullable=False, index=True)
    time_end = Column(DateTime(timezone=False), nullable=False, index=True)

    _query = Column(
        Text,
        nullable=True,
        index=Index(
            'alertation_query',
            'query',
            postgresql_using='gin'
        )
    )

    @hybrid_property
    def query(self):
        return self._query

    @query.expression
    def query(cls):
        return cls._query

    @query.setter
    def query(self, value):
        if isinstance(value, Query):
            self._query = value.compile(
                dialect=postgresql.dialect()
            )
        else:
            self._query = str(value)

    @hybrid_property
    def alteration_type(self):
        return self._alertation_type

    @alteration_type.expression
    def alteration_type(cls):
        return cls._alteration_type

    @alteration_type.setter
    def alteration_type(self, value):
        self._alteration_type = value.strip().lower()

    @property
    def model(self):
        """
        Attempt to import the target model
        """
        classpath = self.target_model.split('.')
        return getattr(
            import_module(
                '.'.join(classpath[:-1])
            ),
            classpath[-1]
        )
