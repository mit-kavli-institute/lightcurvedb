import sys
import os
import logging
import traceback
import multiprocessing as mp
import threading
from yaml import load as yload
from logging import FileHandler


class LCDBLogger(logging.StreamHandler):
    """Multiprocessing logger based off of
    https://gist.github.com/JesseBuesking/10674086.
    Enables queueing of logs into a single file"""

    def __init__(self, name):
        logging.Handler.__init__(self)
        self._handler = FileHandler(name)
        self.q = mp.Queue(-1)

        t = threading.Thread(target=self.receive)
        t.daemon = True
        t.start()

    def setFormatter(self, fmt):
        """Apologies for the camel case in python"""
        logging.Handler.setFormatter(self, fmt)
        self._handler.setFormatter(fmt)

    def receive(self):
        while True:
            try:
                msg = self.q.get()
                self._handler.emit(msg)
            except (KeyboardInterrupt, SystemExit):
                # Program exit
                break
            except EOFError:
                # Queue is done
                break
            except:
                # Error
                traceback.print_exc(file=sys.stderr)

    def send(self, string):
        self.q.put_nowait(string)

    def _format_record(self, record):
        if record.args:
            record.msg = record.msg % record.args
            record.args = None
        if record.exc_info:
            _ = self.format(record)
            record.exc_info = None
        return record

    def emit(self, record):
        try:
            string = self._format_record(record)
            self.send(string)
        except (KeyboardInterrupt, SystemExit):
            # Program exit
            raise
        except:
            self.handleError(record)

    def close(self):
        # cleanup
        self._handler.close()
        logging.Handler.close(self)

PATH = os.path.dirname(os.path.realpath(__file__))
DEFAULT_YAML = os.path.join(PATH, 'default_logging.yaml')
def make_logger(name, config=None):
    # Temporarily import logging config
    import logging.config
    _config = config
    if not _config:
        _config = DEFAULT_YAML
    logger = logging.getLogger(name)
    _config = yload(open(_config, 'rt').read())
    logging.config.dictConfig(_config)
    return logging
