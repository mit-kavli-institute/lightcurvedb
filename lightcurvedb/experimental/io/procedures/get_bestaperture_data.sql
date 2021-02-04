CREATE OR REPLACE FUNCTION get_bestaperture_data(wanted_tic_id bigint) RETURNS SETOF lightpoints AS $$
    DECLARE
        lc_id bigint;
    BEGIN
        SELECT lc.id
        INTO STRICT lc_id
        FROM lightcurves lc
        JOIN best_apertures ba ON
            ba.aperture_id = lc.aperture_id
            AND ba.tic_id = lc.tic_id
        WHERE ba.tic_id = wanted_tic_id AND lc.lightcurve_type_id = 'KSPMagnitude';


        RETURN QUERY SELECT * FROM lightpoints WHERE lightcurve_id = lc_id;
    END;
$$ STABLE ROWS 100000 PARALLEL SAFE LANGUAGE plpgsql;
