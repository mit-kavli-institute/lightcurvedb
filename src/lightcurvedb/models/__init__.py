from .frame import FITSFrame
from .instrument import Instrument
from .interpretation import (
    DetrendingMethod,
    Interpretation,
    PhotometricSource,
    ProcessingGroup,
)
from .observation import Observation, TargetSpecificTime
from .target import Mission, MissionCatalog, Target

__all__ = [
    "FITSFrame",
    "Instrument",
    "PhotometricSource",
    "DetrendingMethod",
    "ProcessingGroup",
    "Observation",
    "Mission",
    "MissionCatalog",
    "Target",
    "TargetSpecificTime",
    "Interpretation",
]

DEFINED_MODELS = __all__
