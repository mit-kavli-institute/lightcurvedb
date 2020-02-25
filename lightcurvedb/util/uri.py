from __future__ import print_function, division
from configparser import ConfigParser

def construct_uri(username, password, db_name, db_host, db_type, port):
    """
        Creates a SQLAlchemy URI connection string
    """
    kwargs = {
        'username': username,
        'password': password,
        'db_name': db_name,
        'db_host': db_host,
        'db_type': db_type,
        'port': port,
    }

    return '{db_type}://{username}:{password}@{db_host}:{port}/{db_name}'.format(
        **kwargs
    )

def uri_from_config(config_path):
    parser = ConfigParser()
    parser.read(config_path)
    kwargs = {
        'username': parser.get('Credentials', 'username'),
        'password': parser.get('Credentials', 'password'),
        'db_name': parser.get('Credentials', 'database_name'),
        'db_host': parser.get('Credentials', 'database_host'),
        'port': parser.get('Credentials', 'database_port'),
    }
    return construct_uri(db_type='postgres+psycopg2', **kwargs)
