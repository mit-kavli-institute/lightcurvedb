from sqlalchemy import Column, DateTime, Integer
from sqlalchemy.ext.hybrid import hybrid_property
from lightcurvedb.core.base_model import QLPReference
from lightcurvedb.core.fields import high_precision_column
from pyquaternion import Quaternion

PYQUAT_KEYWORDS = {
    'q0', 'q1', 'q2', 'q3',
    'w', 'x', 'y', 'z',
    'a', 'b', 'c', 'd'
}


class CameraQuaternion(QLPReference):

    __tablename__ = 'camera_quaternions'

    id = Column(Integer, primary_key=True)
    date = Column(DateTime, index=True)
    camera = Column(Integer, index=True)

    _w = high_precision_column(name='w', nullable=False)
    _x = high_precision_column(name='x', nullable=False)
    _y = high_precision_column(name='y', nullable=False)
    _z = high_precision_column(name='z', nullable=False)


    def __init__(self, *args, **kwargs):
        py_quat_params = {
            k: v for k, v in kwargs.items() if k in PYQUAT_KEYWORDS
        }
        self.quaternion = Quaternion(
            **py_quat_params
        )
        self._w = self.w
        self._x = self.x
        self._y = self.y
        self._z = self.z


    def __getattr__(self, key):
        return getattr(self.quaternion, key)

    # Standard w, x, y, x


    @hybrid_property
    def q0(self):
        return self.w
