from abc import abstractmethod
from pandas import read_csv
from datetime import datetime
import os


def mass_ingest(cursor, filelike, target_table, **options):
    cursor.copy_from(filelike, target_table, **options)


class DataPacker(object):
    """
    A class that describes how data can be bundled together to streamline
    mass data ingestion.

    This class is only effective for data that spans billions to trillions of
    rows. Use with caution.

    Data should arrive in the form of a pandas dataframe or a list of
    dictionaries.
    """

    __target_table__ = "lightpoints"

    def __init__(self, dir_path, pack_options=None):
        self.pack_options = pack_options if pack_options else {}
        self.dir_path = dir_path
        path = os.path.join(
            dir_path,
            "datapack_{0}.blob".format(
                datetime.now().strftime("%Y%m%dT%H%M%S_%f")
            ),
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

    @abstractmethod
    def pack(self, dataframe):
        pass

    @abstractmethod
    def unpack(self):
        pass

    @abstractmethod
    def serialize_to_database(self, lcdb):
        pass


class CSVPacker(DataPacker):
    def pack(self, dataframe):
        if len(dataframe) == 0:
            return
        if not self.has_written_header:
            dataframe.to_csv(self._internal_path, **self.pack_options)
            self.has_written_header = True
            # set permissions
            os.chmod(self._internal_path, 0o664)
        else:
            dataframe.to_csv(
                self._internal_path, mode="a", **self.pack_options
            )

        self.length += len(dataframe)

    def unpack(self):
        return read_csv(self._internal_path)

    def serialize_to_database(self, lcdb):
        if len(self) > 0:
            cursor = lcdb.session.connection().connection.cursor()

            mass_ingest(
                cursor,
                open(self._internal_path, "r"),
                self.__target_table__,
                sep=",",
            )

            # Cleanup :)
            cursor.close()
