from abc import ABCMeta
from pandas import read_csv
from datetime import datetime
import os


class DataPacker(object):
    """
    A class that describes how data can be bundled together to streamline
    mass data ingestion.

    This class is only effective for data that spans billions to trillions of
    rows. Use with caution.

    Data should arrive in the form of a pandas dataframe or a list of
    dictionaries.
    """
    __target_table__ = None

    def __init__(self, dir_path):
        self.dir_path = dir_path
        path = os.path.join(
            dir_path,
            'datapack_{}.blob'.format(
                datetime.now().strftime('%Y%m%dT%H%M%S')        
            )
        )
        self._internal_path = path
        self.session = None
        self.has_written_header = False
        self.length = 0

    def open(self):
        if not os.path.exists(self.dir_path):
            os.makedirs(self.dir_path)

    def close(self):
        os.remove(self._internal_path)
        self.has_written_header = False
        self.length = 0

    def __enter__(self):
        self.open()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __len__(self):
        return self.length

    def pack(self, dataframe):
        if not self.has_written_header:
            dataframe.to_csv(self._internal_path)
            self.has_written_header = True
            # set permissions
            os.chmod(self._internal_path, 0o664)
        else:
            dataframe.to_csv(self._internal_path, mode='a', header=False)

        self.length += len(dataframe)

    def unpack(self):
        return read_csv(self._internal_path)

    def serialize_to_database(self, lcdb):
        cursor = lcdb.session.connection().connection.cursor()

        cursor.copy_from(
            open(self._internal_path, 'r'),
            self.__target_table__,
            sep=','
        )

        # Cleanup :)
        cursor.close()
