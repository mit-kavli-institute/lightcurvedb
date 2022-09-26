"""
This module describes the base manager class and its utility functions.
"""
import cachetools

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
    """
    A manager wraps around a query template in order for it to represent
    an in-memory key-value datastructure. Upon key access the query is resolved
    eagerly unless a cache-hit results in the requested data being immediately
    returned.

    Subclasses will define their query templates as part of their inheritance.

    Examples
    --------
    ```python
    mgr = ManagerClass(db_config)
    x = mgr[identity_value]

    # print(x)
    ```
    """

    def __init__(
        self,
        db_config,
        query_template,
        identity_column,
        cache_class,
        cache_size=1024,
    ):
        """
        Parameters
        ----------
        db_config: pathlike
            A path to an lcdb configuration file
        query_template: sqlalchemy.Query
            A query object which serves as a template.
        identity_column: sqlalchemy.Column
            A related column to the query template which can be considered
            "identifying" or unique.
        """
        self.db_config = db_config
        self.query_template = query_template
        self.identity_column = identity_column

        CacheClass = getattr(cachetools, cache_class)
        self._cache = CacheClass(cache_size)

    def __getitem__(self, key):
        try:
            return self._cache[key]
        except KeyError:
            self.load(key)
            return self._cache[key]

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
