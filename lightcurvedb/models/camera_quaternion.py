from datetime import datetime

from astropy.time import Time, formats
from dateutil import parser
from pyquaternion import Quaternion
from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    Integer,
    UniqueConstraint,
    func,
)
from sqlalchemy.ext.hybrid import hybrid_property

from lightcurvedb.core.base_model import QLPReference
from lightcurvedb.core.fields import high_precision_column
from lightcurvedb.util.constants import GPS_LEAP_SECONDS

PYQUAT_KEYWORDS = {
    "q1",
    "q2",
    "q3",
    "q4",
    "w",
    "x",
    "y",
    "z",
    "a",
    "b",
    "c",
    "d",
}


def get_utc_time(timelike):
    """
    Attempt to convert the timelike into an Astropy Time object in
    UTC time scale.
    """
    if isinstance(timelike, (datetime, Time)):
        # UTC time
        time_in = Time(timelike, scale="utc")
    elif isinstance(timelike, (int, float)):
        time_in = Time(timelike, format="gps")
    elif isinstance(timelike, (str)):
        # attempt to recover
        try:
            gps_like = float(timelike)
            time_in = Time(gps_like, format="gps")
        except ValueError:
            # maybe it looks like a date?
            datetime_like = parser.parse(timelike)
            time_in = Time(datetime_like, scale="utc")
    else:
        raise ValueError(
            "unable to interpret {0}({1}) as astropy.Time".format(
                timelike, type(timelike)
            )
        )
    return Time(time_in, scale="utc")


class TimeUnixLeap(formats.TimeFromEpoch):
    """
    Seconds since 1970-01-01 00:00:00 TAI. This differs from
    'unix' time as it will contain leap seconds.
    """

    name = "unix_leap"
    unit = 1.0 / formats.erfa.DAYSEC
    epoch_val = "1970-01-01 00:00:00"
    epoch_val2 = None
    epoch_scale = "tai"
    epoch_format = "iso"


class CameraQuaternion(QLPReference):
    """
    This class encapsulates Camera orientation via quaternions.
    """

    __tablename__ = "camera_quaternions"

    id = Column(Integer, primary_key=True)
    date = Column(DateTime, index=True, nullable=False)
    camera = Column(Integer, index=True, nullable=False)

    _w = high_precision_column(name="w", nullable=False)
    _x = high_precision_column(name="x", nullable=False)
    _y = high_precision_column(name="y", nullable=False)
    _z = high_precision_column(name="z", nullable=False)

    # Define logical constraints
    __table_args__ = (
        UniqueConstraint("camera", "date"),
        CheckConstraint(
            "camera IN (1, 2, 3, 4)", name="phys_camera_constraint"
        ),
    )

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
        return self._z

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

    @q1.setter
    def q1(self, value):
        self._w = value

    @q2.setter
    def q2(self, value):
        self._x = value

    @q3.setter
    def q3(self, value):
        self._y = value

    @q4.setter
    def q4(self, value):
        self._z = value

    @hybrid_property
    def gps_time(self):
        return Time(get_utc_time(self.date), format="gps")

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

        utc_time = get_utc_time(value)
        self.date = utc_time.datetime

    @gps_time.expression
    def gps_time(cls):
        """
        Task postgresql with calculating GPS time. Values are number of
        seconds since GPS epoch.

        Note
        ----
        This property requires astropy to be updated in order to know the
        number of leap seconds since GPS epoch. See the astropy docs
        to know when this field needs to be updated.
        """
        gps_epoch_offset = 315964800

        return func.date_part("epoch", cls.date) - (
            gps_epoch_offset - GPS_LEAP_SECONDS
        )
