from sqlalchemy import Column, DateTime, Integer
from sqlalchemy.ext.hybrid import hybrid_property
from lightcurvedb.core.base_model import QLPReference
from lightcurvedb.core.fields import high_precision_column
from pyquaternion import Quaternion
from astropy.time import Time
from datetime import datetime


PYQUAT_KEYWORDS = {
    "q0",
    "q1",
    "q2",
    "q3",
    "w",
    "x",
    "y",
    "z",
    "a",
    "b",
    "c",
    "d",
}


class CameraQuaternion(QLPReference):

    __tablename__ = "camera_quaternions"

    id = Column(Integer, primary_key=True)
    date = Column(DateTime, index=True, nullable=False)
    camera = Column(Integer, index=True, nullable=False)

    _w = high_precision_column(name="w", nullable=False)
    _x = high_precision_column(name="x", nullable=False)
    _y = high_precision_column(name="y", nullable=False)
    _z = high_precision_column(name="z", nullable=False)

    def __init__(self, *args, **kwargs):
        py_quat_params = {
            k: v for k, v in kwargs.items() if k in PYQUAT_KEYWORDS
        }
        self.quaternion = Quaternion(**py_quat_params)
        self._w = self.quaternion.w
        self._x = self.quaternion.x
        self._y = self.quaternion.y
        self._z = self.quaternion.z
        if "date" in kwargs:
            self.date = kwargs["date"]
        elif "gps_time" in kwargs:
            self.gps_time = kwargs["gps_time"]
        self.camera = kwargs.get("camera", None)

    # Getters
    # Standard w, x, y, x
    @hybrid_property
    def w(self):
        return self._w

    @hybrid_property
    def x(self):
        return self._x

    @hybrid_property
    def y(self):
        return self._y

    @hybrid_property
    def z(self):
        return self._x

    # Begin extended conversions handled by pyquaternions
    @hybrid_property
    def q1(self):
        return self.w

    @hybrid_property
    def q2(self):
        return self.x

    @hybrid_property
    def q3(self):
        return self.y

    @hybrid_property
    def q4(self):
        return self.z

    @hybrid_property
    def gps_time(self):
        return Time(Time(self.date, scale="utc"), format="gps")

    @gps_time.setter
    def gps_time(self, value):
        """
        Assign GPS time using passed.

        Parameters
        ----------
        value: datetime or float
            Value can be either a datetime or float. If a datetime is passed
            then the value is assumed to be in UTC time. If a float is passed
            then the value is assumed to be in GPS time.
        """

        if isinstance(value, (datetime, Time)):
            # UTC time
            time_in = Time(value, scale="utc")
        elif isinstance(value, (int, float)):
            time_in = Time(value, format="gps")
        else:
            raise ValueError(
                "unable to interpret {0}({1}) as astropy.Time".format(
                    value, type(value)
                )
            )

        self.date = Time(time_in, scale="utc").datetime
