import sys
import os
import logging as __logging
import traceback

lcdb_logger = __logging.getLogger('lightcurvedb')

# create console handler and set level to debug
ch = __logging.StreamHandler()

# create formatter
formatter = __logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
lcdb_logger.addHandler(ch)


def set_level(level):
    __level__ = getattr(__logging, level.upper())
    lcdb_logger.setLevel(__level__)
    ch.setLevel(__level__)

    lcdb_logger.debug('Set logging level to {}'.format(__level__))
