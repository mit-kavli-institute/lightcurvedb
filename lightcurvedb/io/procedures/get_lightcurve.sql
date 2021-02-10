CREATE OR REPLACE FUNCTION
get_lightcurve_data_by_id(
    _lightcurve_id bigint
)
RETURNS SETOF lightpoints AS $$
DECLARE
    partition_id bigint;
    partition_name varchar;
BEGIN
    partition_id := _lightcurve_id / 1000;
    partition_name := FORMAT('partition.lightpoints_%%d_%%d', partition_id, partition_id + 1000);

    RETURN QUERY EXECUTE 'SELECT * FROM ' || partition_name;
END;
$$ STABLE ROWS 100000 PARALLEL SAFE LANGUAGE plpgsql;


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
$$ STABLE ROWS 100000 PARALLEL SAFE LANGUAGE plpgsql;
