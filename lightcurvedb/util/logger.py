import sys
import os
import logging as __logging
import traceback

lcdb_logger = __logging.getLogger('lightcurvedb')

DEFAULT_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


def add_stream_handler(level, fmt=DEFAULT_FORMAT):
    global lcdb_logger
    __level__ = getattr(__logging, level.upper())
    formatter = __logging.Formatter(fmt)
    ch = __logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.setLevel(__level__)
    lcdb_logger.addHandler(ch)
    lcdb_logger.debug('Set {} level to {}'.format(ch, __level__))


def set_level(level):
    __level__ = getattr(__logging, level.upper())
    lcdb_logger.setLevel(__level__)
    lcdb_logger.debug('Set logging level to {}'.format(__level__))
