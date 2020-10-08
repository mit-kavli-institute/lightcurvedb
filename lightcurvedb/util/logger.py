import logging as __logging

lcdb_logger = __logging.getLogger('lightcurvedb')
__SET_STREAM_HANDLER = False
__FILE_LOG_REGISTRY = {}

DEFAULT_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


def add_stream_handler(level, fmt=DEFAULT_FORMAT):
    global lcdb_logger
    global __SET_STREAM_HANDLER

    if __SET_STREAM_HANDLER:
        # Softly ignore
        return

    __level__ = getattr(__logging, level.upper())
    formatter = __logging.Formatter(fmt)
    ch = __logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.setLevel(__level__)
    lcdb_logger.addHandler(ch)
    lcdb_logger.debug('Set {0} level to {1}'.format(ch, __level__))


def add_file_handler(level, filepath, fmt=DEFAULT_FORMAT):
    global lcdb_logger
    global __FILE_LOG_REGISTRY

    if filepath in __FILE_LOG_REGISTRY:
        lcdb_logger.warning(
            "Ignoring duplicate {0} filestream set".format(filepath)
        )

    __level__ = getattr(__logging, level.upper())
    formatter = __logging.Formatter(fmt)
    ch = __logging.FileHandler(filepath)
    ch.setFormatter(formatter)
    ch.setLevel(__level__)
    lcdb_logger.addHandler(ch)
    lcdb_logger.debug(
        'Initialized {0} output at level {1}'.format(filepath, __level__)
    )


def set_level(level):
    __level__ = getattr(__logging, level.upper())
    lcdb_logger.setLevel(__level__)
    lcdb_logger.debug('Set logging level to {0}'.format(__level__))
