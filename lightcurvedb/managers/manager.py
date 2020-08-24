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

    def __get_key__(self, model_inst):
        key = tuple(
            getattr(model_inst, col) for col in self.__uniq_tuple__
        )
        return key

    def __attempt_to_find_key__(self, **kwargs):
        """
        Attempt to create the unique tuble from the passed kwargs.
        """
        return self.__get_key__(kwargs)

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
        self._interior_data[_uniq_key] = model_inst

    def add_model_kw(self, **kwargs):
        pass


def manager_factory(sqlalchemy_model, uniq_col, *additional_uniq_cols):
    class Managed(Manager):
        __managed_class__ = sqlalchemy_model
        __uniq_tuple__ = tuple([uniq_col].extend(additional_uniq_cols))

    return Managed
