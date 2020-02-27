from hypothesis import strategies as st
from hypothesis import given, note
from lightcurvedb.models.lightcurve import Lightcurve, Lightpoint

from .fixtures import db_conn
from .factories import lightcurve as lightcurve_st


@given(lightcurve_st())
def test_cadence_mapping(db_conn, lightcurve):
    try:
        db_conn.session.begin_nested()
        db_conn.add(lightcurve)
        db_conn.commit()

        cadences = lightcurve.cadences
        db_count = db_conn.session.query(Lightpoint).filter(
            Lightpoint.lightcurve_id == lightcurve.id
        ).count()

        note(lightcurve.cadences)

        assert len(cadences) == db_count
        db_conn.session.rollback()
    except:
        db_conn.session.rollback()
        raise

@given(lightcurve_st())
def test_bjd_mapping(db_conn, lightcurve):
    try:
        db_conn.session.begin_nested()
        db_conn.add(lightcurve)
        db_conn.commit()

        barycentric_julian_date = lightcurve.bjd
        db_count = db_conn.session.query(Lightpoint).filter(
            Lightpoint.lightcurve_id == lightcurve.id
        ).count()

        note(lightcurve.bjd)

        assert len(barycentric_julian_date) == db_count
        db_conn.session.rollback()
    except:
        db_conn.session.rollback()
        raise


@given(lightcurve_st())
def test_value_mapping(db_conn, lightcurve):
    try:
        db_conn.session.begin_nested()
        db_conn.add(lightcurve)
        db_conn.commit()

        values = lightcurve.values
        db_count = db_conn.session.query(Lightpoint).filter(
            Lightpoint.lightcurve_id == lightcurve.id
        ).count()

        note(lightcurve.values)

        assert len(values) == db_count
        db_conn.session.rollback()
    except:
        db_conn.session.rollback()
        raise

@given(lightcurve_st())
def test_error_mapping(db_conn, lightcurve):
    try:
        db_conn.session.begin_nested()
        db_conn.add(lightcurve)
        db_conn.commit()

        errors = lightcurve.errors
        db_count = db_conn.session.query(Lightpoint).filter(
            Lightpoint.lightcurve_id == lightcurve.id
        ).count()

        note(lightcurve.errors)

        assert len(errors) == db_count
        db_conn.session.rollback()
    except:
        db_conn.session.rollback()
        raise

@given(lightcurve_st())
def test_x_centroid_mapping(db_conn, lightcurve):
    try:
        db_conn.session.begin_nested()
        db_conn.add(lightcurve)
        db_conn.commit()

        x_centroids = lightcurve.x_centroids
        db_count = db_conn.session.query(Lightpoint).filter(
            Lightpoint.lightcurve_id == lightcurve.id
        ).count()

        note(lightcurve.x_centroids)

        assert len(x_centroids) == db_count
        db_conn.session.rollback()
    except:
        db_conn.session.rollback()
        raise


@given(lightcurve_st())
def test_y_centroid_mapping(db_conn, lightcurve):
    try:
        db_conn.session.begin_nested()
        db_conn.add(lightcurve)
        db_conn.commit()

        y_centroids = lightcurve.y_centroids
        db_count = db_conn.session.query(Lightpoint).filter(
            Lightpoint.lightcurve_id == lightcurve.id
        ).count()

        note(lightcurve.y_centroids)

        assert len(y_centroids) == db_count
        db_conn.session.rollback()
    except:
        db_conn.session.rollback()
        raise
