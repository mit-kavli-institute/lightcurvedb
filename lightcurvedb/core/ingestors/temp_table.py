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

# @listens_for(SQLITE_ENGINE, 'connect')
# def connect(dbapi_connection, connection_record):
#     connection_record.info['pid'] = os.getpid()
# 
# 
# @listens_for(SQLITE_ENGINE, 'checkout')
# def checkout(dbapi_connection, connection_record, connection_proxy):
#     pid = os.getpid()
#     if connection_record.info['pid'] != pid:
#         # connection is being shared across processes...
#         connection_record.connection = connection_proxy.connection = None
#         raise DisconnectionError('Attempting to disassociate database connection')
#     return SQLITE_ENGINE


TemporaryQLPModel = declarative_base()


class LightcurveIDMapper(TemporaryQLPModel):
    __tablename__ = 'lightcurve_id_maps'
    id = Column(BigInteger, primary_key=True)
    tic_id = Column(BigInteger, index=True)
    aperture = Column(String(64), index=True)
    lightcurve_type = Column(String(64), index=True)


class TempObservation(TemporaryQLPModel):
    __tablename__ = 'temp_observations'
    tic_id = Column(BigInteger, primary_key=True)
    orbit_number = Column(Integer, primary_key=True)
    camera = Column(SmallInteger, index=True)
    ccd = Column(SmallInteger, index=True)


class IngestionJob(TemporaryQLPModel):
    __tablename__ = 'ingestion_jobs'
    id = Column(Integer, primary_key=True)
    tic_id = Column(BigInteger, index=True)
    camera = Column(Integer)
    ccd = Column(Integer)
    orbit_number = Column(Integer, index=True)
    file_path = Column(String(256), index=True)

    __table_args__ = {
        'sqlite_autoincrement': True
    }

    def __repr__(self):
        return self.file_path

class TIC8Parameters(TemporaryQLPModel):
    __tablename__ = 'tic8_parameters'
    tic_id = Column(BigInteger, primary_key=True)
    right_ascension = Column(Float)
    declination = Column(Float)
    tmag = Column(Float)
    tmag_error = Column(Float)


TemporaryQLPModel.metadata.create_all(SQLITE_ENGINE)
TempSessionFactory = sessionmaker(bind=SQLITE_ENGINE)
TempSession = scoped_session(TempSessionFactory)
