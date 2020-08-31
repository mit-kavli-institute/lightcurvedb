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
        self._interior_data = dict()
        self._mappers = {
            k: set() for k in self.__uniq_tuple__
        }

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
        return '<{} Manager: {} items>'.format(
            self.__managed_class__,
            len(self._interior_data)
        )

    def __getitem__(self, key):
        """
        Filter the management base down. If a new filter would
        have all the columns have a single mapped instance. Return
        that instance.
        """

    def __get_key__(self, model_inst):
        key = tuple(
            getattr(model_inst, col) for col in self.__uniq_tuple__
        )
        return key

    def __get_key_by_kw__(self, **kwargs):
        try:
            key = tuple(
                kwargs[col] for col in self.__uniq_tuple__
            )
            return key
        except KeyError:
            raise AmbiguousIdentifierDeduction(
                '{} does not contain the needed {} parameters'.format(
                    kwargs.keys(),
                    self.__uniq_tuple__
                )
            )

    def __add_key__(self, key, model_inst):
        self._interior_data[key] = model_inst
        for col in self.__uniq_tuple__:
            mapper = self._mappers[col]
            mapper.add(getattr(model_inst, col))

    def __attempt_to_find_key__(self, **kwargs):
        """
        Attempt to create the unique tuble from the passed kwargs.
        """
        key = []
        for col in self.__uniq_tuple__:
            if not col in kwargs:
                raise AmbiguousIdentifierDeduction(
                    'Unable to find attribute {} in {}'.format(col, kwargs)
                )
            key.append(kwargs[col])
        return tuple(key)

    def __reduce__(self, col):
        raise NotImplementedError

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


def manager_factory(sqlalchemy_model, uniq_col, *additional_uniq_cols):
    cols = [uniq_col]
    cols.extend(additional_uniq_cols)
    class Managed(Manager):
        __managed_class__ = sqlalchemy_model
        __uniq_tuple__ = tuple(cols)

    return Managed
