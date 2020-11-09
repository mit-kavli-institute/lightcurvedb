import struct
import os
from collections import OrderedDict
from sqlalchemy.sql import sqltypes as sql_t
from sqlalchemy.dialects.postgresql import base as psql_t
from io import BytesIO


def get_struct_equivalency(column):
    """
    Return the struct equivalent for the given column.
    Parameters
    ----------
    column : sqlalchemy.Column
        An SQL column to attempt to map.
    Returns
    -------
    str
        A 1 character length string representing its struct
        datatype equivalent.
    Raises
    ------
    ValueError:
        This value cannot be represented as a scalar.
    """
    type_ = column.type

    if isinstance(type_, sql_t.BigInteger):
        return "q"
    elif isinstance(type_, sql_t.Integer):
        return "i"
    elif isinstance(type_, sql_t.SmallInteger):
        return "h"
    elif isinstance(type_, sql_t.Float):
        return "f"
    elif isinstance(type_, psql_t.DOUBLE_PRECISION):
        return "d"
    raise ValueError(
        "Could not find proper struct type for column {0} with "
        "type {1}.".format(
            column.name, column.type_
        )
    )


class Blob(object):

    def __init__(self, scratch_path, name=None):
        self.rows = 0
        self.name = name if name else "process-{0}".format(os.getpid())
        self.blob_path = os.path.join(
            scratch_path,
            "{0}.blob".format(
                self.name
            )
        )

    def __len__(self):
        return self.rows

    def read(self):
        return open(self.blob_path, "rb").read()

    def write(self, buf, n_rows=1):
        with open(self.blob_path, "ab") as out:
            out.write(buf)
        self.rows += n_rows


class RowWiseBlob(Blob):
    def __init__(self, struct_fmt, scratch_path, name=None):
        super(RowWiseBlob, self).__init__(scratch_path, name=name)
        self.packer = struct.Struct(struct_fmt)

    def load_row(self, *values):
        bundle = self.packer.pack(*values)
        self.write(bundle)

    def load_rows(self, value_array):
        buf = BytesIO()
        length = 0
        for values in value_array:
            bundle = self.packer.pack(*values)
            buf.write(bundle)
            length += 1
        self.write(buf, n_rows=length)


class Blobable(object):
    @classmethod
    def struct_fmt(cls):
        columns = tuple(cls.__table__.columns)
        struct_format = ''.join(
            map(get_struct_equivalency, columns)
        )
        return struct_format

    @classmethod
    def struct(cls):
        return struct.Struct(cls.struct_format)

    @classmethod
    def struct_size(cls):
        return cls.struct().size

    def pack_tuples(cls, data_rows):
        packer = cls.struct()

        for row in data_rows:
            yield packer.pack(*row)

    def pack_dictionaries(self, data_rows):
        #  Transform into tuples
        columns = tuple(cls.__table__.columns)
        tuples = []

        for row in data_rows:
            tuples.append(
                tuple(row[column.name] for column in columns)
            )
        return cls.pack_tuples(tuples)
