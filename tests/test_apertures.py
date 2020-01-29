# -*- coding: utf-8 -*-
from hypothesis import strategies as st
from hypothesis import given
from lightcurvedb.models.aperture import Aperture

from .fixtures import db_conn
from .factories import aperture as aperture_st
from .factories import postgres_text
from .constants import CONFIG_PATH


aperture_signature = (
    postgres_text(),
    st.floats(allow_nan=False, allow_infinity=False),
    st.floats(allow_nan=False, allow_infinity=False),
    st.floats(allow_nan=False, allow_infinity=False)
)

@given(*aperture_signature)
def test_aperture_instantiation(name, star_r, inner_r, outer_r):
    aperture = Aperture(name=name, star_radius=star_r, inner_radius=inner_r, outer_radius=outer_r)

    assert aperture.name == name
    assert aperture.star_r == star_r
    assert aperture.inner_r == inner_r
    assert aperture.outer_r == outer_r

@given(aperture_st())
def test_aperture_retrieval(db_conn, aperture):
    db_conn.session.begin_nested()
    db_conn.add(aperture)
    db_conn.commit()
    q = db_conn.session.query(Aperture).get(aperture.id)
    assert q is not None
    db_conn.session.rollback()