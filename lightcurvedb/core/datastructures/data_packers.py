from abc import abstractmethod
from pandas import read_csv
from datetime import datetime
from lightcurvedb.core.datastructures.blob import Blob, RowWiseBlob
from lightcurvedb.models import Observation, Lightpoint
import os
import struct
from io import BufferedIOBase
from tabulate import tabulate


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


class LightpointPartitionBlob(Blob):
    pass


class LightpointPartitionWriter(LightpointPartitionBlob):
    def __init__(self, partition_start, partition_end, scratch_path):
        name = "partition_{0}_{1}".format(partition_start, partition_end)

        super(LightpointPartitionBlob, self).__init__(scratch_path, name=name)

        self.partition_start = partition_start
        self.partition_end = partition_end

        self.observation_blob = RowWiseBlob(
            Observation.struct_fmt(),
            scratch_path,
            name="obs_{0}_{1}".format(partition_start, partition_end)
        )
        self.lightpoint_blob = RowWiseBlob(
            Lightpoint.struct_fmt(),
            scratch_path,
            name="lp_{0}_{1}".format(partition_start, partition_end)
        )

    def add_observations(self, observations):
        pack_iter = Observation.pack_dictionaries(observations)
        self.observation_blob.load_rows(pack_iter)

    def add_lightpoints(self, lightpoint_df):
        pack_iter = Lightpoint.pack_tuples(
            lightpoint_df.to_records()
        )
        self.lightpoint_blob.load_rows(pack_iter)

    def write(self):
        # Write preamble
        fmt = 'IIII'
        preamble_bytes = struct.pack(
            fmt,
            self.partition_start,
            self.partition_end,
            len(self.observation_blob),
            len(self.lightpoint_blob)
        )
        with open(self.blob_path, "wb") as out:
            out.write(preamble_bytes)
            if len(self.lightpoint_blob):
                out.write(self.lightpoint_blob.read())
            if len(self.observation_blob):
                out.write(self.observation_blob.read())

        # Finished successfully, remove temporary file
        if len(self.lightpoint_blob):
            os.remove(self.lightpoint_blob.blob_path)
        if len(self.observation_blob):
            os.remove(self.observation_blob.blob_path)


class LightpointPartitionReader(LightpointPartitionBlob):
    def __init__(self, path):
        self.blob_path = path

    def __repr__(self):
        with open(self.blob_path, "rb") as fin:
            preamble = self.__get_preamble__(fin)
            partition_start = preamble[0]
            partition_end = preamble[1]
            n_obs = preamble[2]
            n_lightpoints = preamble[3]
        return (
            "LP_Partition {0}-{1}: "
            "{2} Observations {3} Lightpoints"
        ).format(
            partition_start,
            partition_end,
            n_obs,
            n_lightpoints
        )

    def __get_preamble__(self, fd):
        preamble = struct.unpack('IIII', fd)
        return preamble

    def __obs_iter__(self, fd, n_obs):
        loader = Observation.struct()

        for _ in range(n_ops):
            yield loader.unpack(fd)

    def __lp_iter__(self, fd, n_lps):
        loader = Lightpoint.struct()
        for _ in range(n_lps):
            yield loader.unpack(fd)

    def print_lightpoints(self, db, **fmt_args):
        with open(self.blob_path, "rb") as fin:
            preamble = self.__get_preamble__(fin)
            lp_iter = self.__lp_iter__(fin, preamble[3])
            columns = [c.name for c in Lightpoint.__table__.columns]

            return tabulate(lp_iter, header=columns, **fmt_args)

    def print_observations(self, db, **fmt_args):
        with open(self.blob_path, "rb") as fin:
            preamble = self.__get_preamble__(fin)
            # skip lightpoints
            offset = preamble[3] * Lightpoint.struct_size
            fin.seek(offset)
            columns = [c.name for c in Observation.__table__.columns]
            obs_iter = self.__obs_iter__(find, preamble[2])
            return tabulate(obs_iter, header=columns, **fmt_args)

    def upload(self, db):
        with open(self.blob_path, "rb") as fin:
            preamble = self.__get_preamble__(fin)
            partition_start = preamble[0]
            partition_end = preamble[1]
            n_obs = preamble[2]
            n_lightpoints = preamble[3]

            partition = "partitions.{0}_{1}_{2}".format(
                Lightpoint.__tablename__,
                partition_start,
                partition_end
            )

            # Copy lightpoints
            columns = [c.name for c in Lightpoint.__table__.columns]
            mgr = CopyManager(
                db.session.connection().connection,
                partition,
                columns
            )
            mg.threaded_copy(
                self.__lp_iter__(fin, n_lightpoints)
            )

            # Observations can be inserted normally
            columns = [c.name for c in Observation.__table__.columns]

            obs_df.DataFrame(
                self.__obs_iter__(fin, n_obs),
                columns=columns
            ).set_index(['tic_id', 'orbit_id']).sort_index()

            # Remove duplication
            obs_df = obs_df[~obs_df.index.duplicated(keep='last')]
            q = Observation.upsert_q()

            db.session.execute(
                Observation.upsert_q(),
                obs_df.reset_index().to_dict('records')
            )

            db.commit()

    def write(self, *args, **kwargs):
        # Invalid operation
        raise RuntimeError("Cannot write from reader")
