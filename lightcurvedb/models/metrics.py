from sqlalchemy import Column, ForeignKey, Integer, String, BigInteger, Float, Sequence, Text, SmallInt, Sequence, Index, DateTime
from sqlalchemy.dialects import postgresql
from lightcurvedb.core.base_model import QLPMetric


class QLPProcess(QLPMetric):
    __tablename__ = 'qlpprocesses'
    id = Column(Integer, Sequence('qlpprocess_id_seq'), primary_key=True)
    job_type = Column(String(255), index=True)
    job_version_major = Column(SmallInt, index=True, nullable=False)
    job_version_minor = Column(SmallInt, index=True, nullable=False)
    job_version_revision = Column(Integer, index=True, nullable=False)
    job_description = Text()


class QLPAlteration(QLPMetric):
    __tablename__ = 'qlpalterations'
    id = Column(BigInteger, Sequence('qlpaleration_id_seq'), cache=1000), primary_key=True)
    process_id = Column(ForeignKey(QLPProcess), nullable=False)
    target_model = Column(
        String(255),
        index=Index(
            'alteration_model',
            'target_model',
            postgresql_using='gin'
        ),
        nullable=False
    )
    alteration_type = Column(String(255), index=True, nullable=False)
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
