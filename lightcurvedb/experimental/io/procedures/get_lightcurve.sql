CREATE OR REPLACE FUNCTION
get_lightcurve_data(
    tic_id biginteger,
    aperture varchar,
    lightcurve_type varchar
)
RETURNS SETS lightpoint AS $$
    DECLARE
        lc_id biginteger;
    BEGIN
        SELECT lightcurves.id INTO STRICT lc_id WHERE lightcurves.tic_id = tic_id
        RETURN QUERY SELECT * FROM lightpoints WHERE lightcurve_id = lc_id;
    END;
$$ STABLE ROWS 100000 PARALLEL SAFE LANGUAGE plpgsql;
