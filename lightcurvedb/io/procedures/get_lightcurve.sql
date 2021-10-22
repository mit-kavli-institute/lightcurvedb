CREATE OR REPLACE FUNCTION
get_lightcurve_data_by_id(
    _lightcurve_id bigint
)
RETURNS SETOF lightpoints AS $$
    BEGIN
        RETURN QUERY SELECT DISTINCT ON (cadence) * FROM lightpoints WHERE lightcurve_id IN (_lightcurve_id, -1) ORDER BY cadence ASC;
    END;
$$ STABLE ROWS 10000 PARALLEL SAFE LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION
get_lightcurve_data(
    tic_id bigint,
    aperture varchar,
    lightcurve_type varchar
)
RETURNS SETOF lightpoints AS $$
    DECLARE
        lc_id bigint;
    BEGIN
        SELECT lightcurves.id INTO STRICT lc_id FROM lightcurves WHERE lightcurves.tic_id = tic_id;
        RETURN QUERY SELECT * FROM get_lightcurve_data_by_id(lc_id);
    END;
$$ STABLE ROWS 10000 PARALLEL SAFE LANGUAGE plpgsql;
