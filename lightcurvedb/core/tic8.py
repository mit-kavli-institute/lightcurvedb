import os
import sys
import warnings
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import DisconnectionError
from sqlalchemy.event import listens_for
from sqlalchemy import create_engine, Table

CONFIG_PATH = os.path.expanduser(
    os.path.join('~', '.config', 'tsig', 'tic-dbinfo')
)

TIC8_CONFIGURATION = {
    'pool_size': 10,
    'max_overflow': -1
}

try:
    for line in open(CONFIG_PATH, 'rt').readlines():
        key, value = line.strip().split('=')
        key = key.strip()
        value = value.strip()
        TIC8_CONFIGURATION[key] = value
except FileNotFoundError:
    sys.stderr.write(
        '{} was not found, please check your configuration environment\n'.format(CONFIG_PATH)
    )
    sys.exit(1)

TIC8_Base = automap_base()
TIC8_ENGINE = create_engine(
    'postgresql://{dbuser}:{dbpass}@{dbhost}/{dbname}'.format(
        **TIC8_CONFIGURATION
    )
)


@listens_for(TIC8_ENGINE, 'connect')
def connect(dbapi_connection, connection_record):
    connection_record.info['pid'] = os.getpid()


@listens_for(TIC8_ENGINE, 'checkout')
def checkout(dbapi_connection, connection_record, connection_proxy):
    pid = os.getpid()
    if connection_record.info['pid'] != pid:
        connection_record.connection = connection_proxy.connection = None
        raise DisconnectionError('Attempting to disassociate database connection')


TIC8_Base.prepare(TIC8_ENGINE)
TIC_Entries  = Table('ticentries', TIC8_Base.metadata, autoload=True, autoload_with=TIC8_ENGINE)
