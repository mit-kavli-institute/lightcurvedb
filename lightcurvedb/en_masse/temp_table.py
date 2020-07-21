from sqlalchemy import Table, Column, BigInteger
from sqlalchemy.orm import Query
from sqlalchemy.inspection import inspect
from sqlalchemy.sql import compiler
from lightcurvedb.core.base_model import QLPModel
import os
from datetime import datetime

DATETIME_EPOCH = datetime.utcfromtimestamp(0)


def seconds_since_epoc():
    cur_time = datetime.utcnow()
    elapsed = cur_time - DATETIME_EPOCH
    return int(elapsed.total_seconds())


def declare_lightcurve_cadence_map(*additional_columns, **table_kwargs):

    tablename = 'temp_{}_{}'.format(
        os.getpid(),
        seconds_since_epoc()
    )

    table = Table(
        tablename,
        QLPModel.metadata,
        Column('lightcurve_id', BigInteger, primary_key=True),
        Column('cadence', BigInteger, primary_key=True),
        *additional_columns,
        **table_kwargs
    )

    return table


def tic_temp_table(session, metadata, tics):
    tablename = '{}_{}'.format('tic_tmp', os.getpid())
    temp_table = Table(
        tablename,
        metadata,
        Column('tic_id', BigInteger, primary_key=True),
        prefixes=['TEMPORARY']
    )
    temp_table.create(bind=session.bind)
    session.commit()

    insertion_q = temp_table.insert().values(
        [{'tic_id': tic} for tic in tics]
    )

    session.execute(insertion_q)

    return temp_table
