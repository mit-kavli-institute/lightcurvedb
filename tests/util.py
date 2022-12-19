def import_lc_prereqs(db, lightcurves):
    for lc in lightcurves:
        db.merge(lc.aperture)
        db.merge(lc.lightcurve_type)
