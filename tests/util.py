def import_lc_prereqs(db, lightcurves):
    for lc in lightcurves:
        db.session.merge(lc.aperture)
        db.session.merge(lc.lightcurve_type)
