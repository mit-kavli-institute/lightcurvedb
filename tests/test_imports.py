def test_import_base():
    import lightcurvedb as lcdb
    assert lcdb is not None

def test_import_models_module():
    from lightcurvedb import models
    assert models is not None
    assert models.Lightcurve is not None
    assert models.Aperture is not None
    assert models.LightcurveType is not None
    assert models.Frame is not None
    assert models.FrameType is not None


def test_helper_imports():
    from lightcurvedb import DB
    from lightcurvedb import db_from_config
    assert DB is not None
    assert db_from_config is not None
