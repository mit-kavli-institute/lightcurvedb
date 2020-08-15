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


#SQLITE_ENGINE = create_engine(
#    'sqlite:///:memory:',
#    connect_args={'check_same_thread': False},
#    poolclass=StaticPool
#)

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


class FileObservation(TemporaryQLPModel):
    __tablename__ = 'fileobservations'
    __table_args__ = {
        'sqlite_autoincrement': True
    }

    id = Column(Integer, primary_key=True)
    tic_id = Column(BigInteger, index=True, nullable=False)
    camera = Column(Integer, index=True, nullable=False)
    ccd = Column(Integer, index=True, nullable=False)
    orbit_number = Column(Integer, index=True, nullable=False)
    file_path = Column(String(255), unique=True, nullable=False)


class TempObservation(TemporaryQLPModel):
    __tablename__ = 'temp_observations'
    tic_id = Column(BigInteger, primary_key=True)
    orbit_number = Column(Integer, primary_key=True)
    camera = Column(SmallInteger, index=True, nullable=False)
    ccd = Column(SmallInteger, index=True, nullable=False)

    def to_h5_path(self, base_path='/pdo/qlp-data'):
        return os.path.join(
            base_path,
            'orbit-{}'.format(self.orbit_number),
            'cam{}'.format(self.camera),
            'ccd{}'.format(self.ccd),
            'LC',
            '{}.h5'.format(self.tic_id)
        )

    @property
    def to_dict(self):
        return dict(
            tic_id=self.tic_id,
            orbit_number=self.orbit_number,
            camera=self.camera,
            ccd=self.ccd,
            file_path=self.to_h5_path()
        )

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


class QualityFlags(TemporaryQLPModel):
    __tablename__ = 'quality_flags'
    cadence = Column(BigInteger, primary_key=True)
    camera = Column(Integer, primary_key=True)
    ccd = Column(Integer, primary_key=True)
    quality_flag = Column(Integer, index=True)


#TemporaryQLPModel.metadata.create_all(SQLITE_ENGINE)
#TempSessionFactory = sessionmaker(bind=SQLITE_ENGINE)
#TempSession = scoped_session(TempSessionFactory)
