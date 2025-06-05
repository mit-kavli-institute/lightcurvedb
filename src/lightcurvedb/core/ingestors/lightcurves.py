import numpy as np
from sqlalchemy.exc import IntegrityError


def get_or_create(db, Model, **kwargs):
    instance = db.query(Model).filter_by(**kwargs).one_or_none()
    if instance is None:
        try:
            instance = Model(**kwargs)
            db.add(instance)
            db.commit()
        except IntegrityError:
            # Assume another instance has already
            instance = db.query(Model).filter_by(**kwargs).one_or_none()
    return instance


def get_cadences(h5_fd):
    lightcurve = h5_fd["LightCurve"]
    return lightcurve["Cadence"][()].astype(int)


def get_barycentric_julian_dates(h5_fd):
    lightcurve = h5_fd["LightCurve"]
    return lightcurve["BJD"][()]


def get_best_detrending_type(h5_fd, default_key="KSPMagnitude"):
    photometry = h5_fd["LightCurve"]["AperturePhotometry"]
    if "bestdmagkey" in photometry.attrs:
        return photometry.attrs["bestdmagkey"]
    elif "primarydetrending" in photometry.attrs:
        return photometry.attrs["primarydetrending"] + "Magnitude"
    else:
        return default_key


def iterate_for_raw_data(h5_fd):
    """
    Yield over the given h5 data for orbit lightcurve lightpoint
    data.
    """
    lightcurve = h5_fd["LightCurve"]
    cadences = get_cadences(h5_fd)
    barycentric_julian_dates = get_barycentric_julian_dates(h5_fd)

    skip_these_tokens = {"error", "x", "y"}
    for aperture_name, photometry in lightcurve["AperturePhotometry"].items():
        if "name" in photometry.attrs:
            aperture_name = photometry.attrs["name"]
        flux_weighted_x_centroid = photometry["X"][()]
        flux_weighted_y_centroid = photometry["Y"][()]
        for type_name, timeseries in photometry.items():
            if any(token in type_name.lower() for token in skip_these_tokens):
                continue
            expected_error_name = f"{type_name}Error"

            data = timeseries[()]
            try:
                error = photometry[expected_error_name][()]
            except KeyError:
                error = np.full_like(data, np.nan)

            raw_data = {
                "cadence": cadences,
                "barycentric_julian_date": barycentric_julian_dates,
                "data": data,
                "error": error,
                "x_centroid": flux_weighted_x_centroid,
                "y_centroid": flux_weighted_y_centroid,
            }
            yield aperture_name, type_name, raw_data

    # Yield Expected Background Time Series
    aperture_name = "BackgroundAperture"
    type_name = "Background"
    x_centroids = lightcurve["X"][()]
    y_centroids = lightcurve["Y"][()]
    raw_background_data = {
        "cadence": cadences,
        "barycentric_julian_date": barycentric_julian_dates,
        "data": lightcurve[type_name]["Value"][()],
        "error": lightcurve[type_name]["Error"][()],
        "x_centroid": x_centroids,
        "y_centroid": y_centroids,
    }

    yield aperture_name, type_name, raw_background_data
