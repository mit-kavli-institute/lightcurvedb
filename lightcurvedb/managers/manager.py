from lightcurvedb import db_from_config
from lightcurvedb.exceptions import LightcurveDBException


class AmbiguousIdentifierDeduction(LightcurveDBException):
    """Raised when resolving a lightcurve ID and an insufficent
    set of identifiers were passed to successfully determine a scalar
    ID or lack thereof.
    """

    pass


class DuplicateEntryException(LightcurveDBException):
    """Raised when attempting to add a lightcurve which already
    exists in a LightcurveManager context.
    """

    pass


class BaseManager:
    def __init__(self, db_config, query_template, identity_column):
        self.db = db_from_config(db_config)
        self.query_template = query_template
        self.identity_column = identity_column
        self._cache = {}

    def __getitem__(self, key):
        raise NotImplementedError

    def __iter__(self):
        for key, value in self._cache.items():
            yield key, value

    def interpret_data(self, data_aggregate):
        return data_aggregate

    def map(self, func):
        for _, value in self:
            yield func(value)

    def evict(self, id):
        return self._cache.pop(id)

    def load(self, id):
        with self.db as db:
            q = self.query_template.filter(self.identity_column == id)
            for id_, *data in db.execute(q):
                self._cache[id] = self.interpret_data(data)
