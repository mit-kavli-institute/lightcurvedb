from hypothesis import strategies as st
from hypothesis import given, note, assume
from lightcurvedb.models.lightcurve import Lightcurve, Lightpoint

from .fixtures import db_conn
from .factories import lightcurve as lightcurve_st

