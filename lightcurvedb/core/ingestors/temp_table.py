import os
import pandas as pd
from itertools import product
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from sqlalchemy.event import listens_for
from sqlalchemy.exc import DisconnectionError
from sqlalchemy import Column, Boolean, BigInteger, Integer, SmallInteger, Float, String, func, select, bindparam, and_, update
from sqlalchemy.ext.declarative import declared_attr, declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker


SQLITE_ENGINE = create_engine(
    'sqlite:///:memory:',
    connect_args={'check_same_thread': False},
    poolclass=StaticPool
)

TemporaryQLPModel = declarative_base()


class LightcurveIDMapper(TemporaryQLPModel):
    __tablename__ = 'lightcurve_id_maps'
    id = Column(BigInteger, primary_key=True)
    tic_id = Column(BigInteger, index=True, nullable=False)
    aperture = Column(String(64), index=True, nullable=False)
    lightcurve_type = Column(String(64), index=True, nullable=False)

    @property
    def to_key_value(self):
        return (self.tic_id, self.aperture, self.lightcurve_type), self.id


class TempObservation(TemporaryQLPModel):
    __tablename__ = 'temp_observations'
    tic_id = Column(BigInteger, primary_key=True)
    orbit_number = Column(Integer, primary_key=True)
    camera = Column(SmallInteger, index=True, nullable=False)
    ccd = Column(SmallInteger, index=True, nullable=False)


class IngestionJob(TemporaryQLPModel):
    __tablename__ = 'ingestion_jobs'
    id = Column(Integer, primary_key=True)
    tic_id = Column(BigInteger, index=True, nullable=False)
    camera = Column(Integer, nullable=False)
    ccd = Column(Integer, nullable=False)
    orbit_number = Column(Integer, index=True, nullable=False)
    file_path = Column(String(256), index=True, nullable=False)

    __table_args__ = {
        'sqlite_autoincrement': True
    }

    def __repr__(self):
        return self.file_path

class TIC8Parameters(TemporaryQLPModel):
    __tablename__ = 'tic8_parameters'
    tic_id = Column(BigInteger, primary_key=True)
    right_ascension = Column(Float, nullable=False)
    declination = Column(Float, nullable=False)
    tmag = Column(Float, nullable=False)
    tmag_error = Column(Float)


TemporaryQLPModel.metadata.create_all(SQLITE_ENGINE)
TempSessionFactory = sessionmaker(bind=SQLITE_ENGINE)
TempSession = scoped_session(TempSessionFactory)