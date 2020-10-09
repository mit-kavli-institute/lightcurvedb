from collections import defaultdict
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


class Manager(object):
    """Base Manager object. Defines abstract methods to retrive, store,
    and update lightcurves."""

    __managed_class__ = None
    __uniq_tuple__ = None

    def __init__(self, initial_models):
        self._interior_data = {}
        self._mappers = {k: defaultdict(set) for k in self.__uniq_tuple__}

        for model in initial_models:
            self.add_model(model)

    def __len__(self):
        return len(self._interior_data)

    def __contains__(self, obj):
        """
        Checks to see if the given obj is in the Manager. If this obj is
        an instance of the `tracked` class then this method checks if this
        object is tracked verbatim. Otherwise, this object is assumed to be
        a unique key within the Manager.
        """
        if isinstance(obj, self.__managed_class__):
            key = self.__get_key__(obj)
        else:
            key = obj
        return key in self._interior_data

    def __repr__(self):
        return "<{0} Manager: {1} items>".format(
            self.__managed_class__, len(self._interior_data)
        )

    def __getitem__(self, scalar_key):
        """
        Filter the management base down. If a new filter would
        have all the columns have a single mapped instance. Return
        that instance.
        """
        keys = set()

        for _, pair_mappings in self._mappers.items():
            try:
                tuple_identifiers = pair_mappings[scalar_key]
                keys.update(tuple_identifiers)
            except KeyError:
                continue

        if len(keys) == 0:
            raise KeyError(
                "Key {0} not found in any of the "
                "tracked keys in {1}.".format(scalar_key, self)
            )
        if len(keys) == 1:
            return self._interior_data[next(iter(keys))]

        # Return a new Manager with the filtered items
        return self.__class__(self._interior_data[key] for key in keys)

    def __get_key__(self, model_inst):
        key = tuple(getattr(model_inst, col) for col in self.__uniq_tuple__)
        return key

    def __get_key_by_kw__(self, **kwargs):
        try:
            key = tuple(kwargs[col] for col in self.__uniq_tuple__)
            return key
        except KeyError:
            raise AmbiguousIdentifierDeduction(
                "{0} does not contain the needed {1} parameters".format(
                    kwargs.keys(), self.__uniq_tuple__
                )
            )

    def __add_key__(self, key, model_inst):
        self._interior_data[key] = model_inst
        for ith, col in enumerate(self.__uniq_tuple__):
            mapper = self._mappers[col]
            internal_key = key[ith]
            mapper[internal_key].add(key)

    def __attempt_to_find_key__(self, **kwargs):
        """
        Attempt to create the unique tuble from the passed kwargs.
        """
        key = []
        for col in self.__uniq_tuple__:
            if col not in kwargs:
                raise AmbiguousIdentifierDeduction(
                    "Unable to find attribute {0} in {1}".format(col, kwargs)
                )
            key.append(kwargs[col])
        return tuple(key)

    def __reduce__(self, col):
        raise NotImplementedError

    def __iter__(self):
        return iter(self._interior_data.values())

    def get_model(self, val, *uniq_vals):
        key = tuple([val].extend(uniq_vals))
        return self._interior_data[key]

    def add_model(self, model_inst):
        """
        Add the model to be tracked by the Manager
        """
        _uniq_key = self.__get_key__(model_inst)
        if _uniq_key in self._interior_data:
            raise DuplicateEntryException()
        self.__add_key__(_uniq_key, model_inst)

        return model_inst

    def add_model_kw(self, **kwargs):
        self.__attempt_to_find_key__(**kwargs)
        # **kwargs contains a valid key
        instance = self.__managed_class__(**kwargs)
        return self.add_model(instance)

    @property
    def keys(self):
        """
        Returns a setlike collection of all the primary
        identifiers used to discern individual items.

        Returns
        -------
        set
        """
        return set(self._interior_data.keys())


def manager_factory(sqlalchemy_model, uniq_col, *additional_uniq_cols):
    cols = [uniq_col]
    cols.extend(additional_uniq_cols)

    class Managed(Manager):
        __managed_class__ = sqlalchemy_model
        __uniq_tuple__ = tuple(cols)

    return Managed
