from __future__ import print_function, division

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