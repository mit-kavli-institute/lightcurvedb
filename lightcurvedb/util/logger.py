import logging as logging
import tqdm

lcdb_logger = logging.getLogger("lightcurvedb")
__SET_STREAM_HANDLER = False
__FILE_LOG_REGISTRY = {}

DEFAULT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def get_lcdb_logging(name="lightcurvedb", handlers=None):
    logger = logging.getLogger(name)

    if handlers:
        for handle in handlers:
            logger.addHandle(handle)

    return logger


class TQDMLoggingHandler(logging.Handler):
    """
    Provide an easy handler that is compatible with tqdm
    """
    def __init__(self, level="INFO"):
        level = getattr(logging, level)
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.tqdm.write(msg)
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


def add_stream_handler(level, fmt=DEFAULT_FORMAT):
    if __SET_STREAM_HANDLER:
        # Softly ignore
        return

    __level__ = getattr(logging, level.upper())
    formatter = logging.Formatter(fmt)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.setLevel(__level__)
    lcdb_logger.addHandler(ch)
    lcdb_logger.debug("Set {0} level to {1}".format(ch, __level__))


def add_file_handler(level, filepath, fmt=DEFAULT_FORMAT):
    global lcdb_logger
    global __FILE_LOG_REGISTRY

    if filepath in __FILE_LOG_REGISTRY:
        lcdb_logger.warning(
            "Ignoring duplicate {0} filestream set".format(filepath)
        )

    __level__ = getattr(logging, level.upper())
    formatter = logging.Formatter(fmt)
    ch = logging.FileHandler(filepath)
    ch.setFormatter(formatter)
    ch.setLevel(__level__)
    lcdb_logger.addHandler(ch)
    lcdb_logger.debug(
        "Initialized {0} output at level {1}".format(filepath, __level__)
    )


def add_tqdm_handler(logger, level, fmt=DEFAULT_FORMAT):
    handler = TQDMLoggingHandler(level=level)
    logger.addHandler(handler)
    logger.debug(
        "Initialized TQDM compatible logging handler"
    )
    return logger


def set_level(level):
    __level__ = getattr(logging, level.upper())
    global lcdb_logger
    lcdb_logger.setLevel(__level__)
    lcdb_logger.debug("Set logging level to {0}".format(__level__))
