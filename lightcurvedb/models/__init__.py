from sqlalchemy.ext.associationproxy import association_proxy as _ap
from sqlalchemy.orm import relationship as _rel
from .observations import Observation
from .aperture import Aperture, BestApertureMap
from .camera_quaternion import CameraQuaternion
from .frame import FrameType, Frame
from .orbit import Orbit
from .lightpoint import Lightpoint
from .lightcurve import Lightcurve, LightcurveType
from .spacecraft import SpacecraftEphemris
from .bls import BLS
from .metrics import QLPProcess, QLPAlteration


# Configure relationships to avoid circular dependencies
# Observation.lightcurve = _rel(Lightcurve, back_populates="observations")
# Observation.orbit = _rel(Orbit, back_populates="observations")
#
# Lightcurve.observations = _rel(Observation, back_populates="lightcurve")
# Lightcurve.orbits = _ap("observations", "orbit")
#
# Orbit.observations = _rel(Observation, back_populates="orbit")
# Orbit.lightcurves = _ap("observations", "lightcurve")


__all__ = [
    "Aperture",
    "BestApertureMap",
    "BLS",
    "CameraQuaternion",
    "FrameType",
    "Frame",
    "Orbit",
    "Lightcurve",
    "LightcurveType",
    "Lightpoint",
    "Observation",
    "SpacecraftEphemris",
    "QLPProcess",
    "QLPAlteration",
]

DEFINED_MODELS = __all__
