import os
import sys
import warnings
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import DisconnectionError, SAWarning
from sqlalchemy.event import listens_for
from sqlalchemy import create_engine, Table

CONFIG_PATH = os.path.expanduser(
        os.path.join('~', '.config', 'tsig', 'tic-dbinfo')
        )

TIC8_CONFIGURATION = {
        'pool_size': 10,
        'max_overflow': -1,
        'executemany_mode': 'values',
        'executemany_values_page_size': 10000,
        'executemany_batch_page_size': 500
        }

try:
    for line in open(CONFIG_PATH, 'rt').readlines():
        key, value = line.strip().split('=')
        key = key.strip()
        value = value.strip()
        TIC8_CONFIGURATION[key] = value
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=SAWarning)

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
                connection_record.connection = None
                connection_proxy.connection = None
                raise DisconnectionError(
                    'Attempting to disassociate database connection'
                )

        TIC8_Base.prepare(TIC8_ENGINE)
        TIC_Entries = Table(
                'ticentries',
                TIC8_Base.metadata,
                autoload=True,
                autoload_with=TIC8_ENGINE
        )
except IOError:
    sys.stderr.write((
        '{0} was not found, '
        'please check your configuration environment\n'.format(CONFIG_PATH)
    ))
    TIC8_Base = None
    TIC8_ENGINE = None
    TIC_Entries = None
