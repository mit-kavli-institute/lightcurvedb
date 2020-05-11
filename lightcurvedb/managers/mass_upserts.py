from lightcurvedb.core.base_model import QLPModel
from collections import defaultdict


class MassUpsert(object):
    def __init__(self):
        self._mappings = {}
        self.mapping_cache = defaultdict(list)

    @classmethod
    def init_with_base(cls):
        """Generate a MassUpsert object using all the subclasses of QLPModel
        """
        subclasses = list(cls.__subclasses__())
        instance = cls()

        for subclass in subclasses:
            instance.add_mapping(subclass)

        return instance

    def add_table_mapping(self, key, table):
        self._mappings[key] = table

    def add_mapping(self, mapping):
        self.add_table_mapping(mapping.__tablename__, mapping.table)

    def get_table(self, ref):
        if isinstance(ref, QLPModel):
            return self._mappings[ref.__tablename__]
        elif isinstance(ref, str):
            return self._mappings[ref]
        raise TypeError(
            'passed ref must be a subclass of QLPModel or str, was given {}'.format(
                type(ref)
            )
        )

    def add(self, ref, **sql_columns):
        table = self._mappings[ref]
        self.mapping_cache[table].append(sql_columns)

    def update_q(self, ref):
        raise NotImplementedError

    def insert_q(self, ref):
        raise NotImplementedError

    def upsert_q(self, ref):
        raise NotImplementedError