CREATE OR REPLACE FUNCTION get_bestaperture_data(tic_id biginteger) RETURNS SETS LIGHTPOINT AS $$
    DECLARE
        lc_id biginteger;
        SELECT lightcurves.id
        INTO STRICT lc_id
        JOIN bestapertures ba ON
            ba.aperture_id = lc.aperture_id
            AND ba.tic_id = lc.tic_id
        WHERE ba.tic_id = tic_id AND lc.lightcurve_type = 'KSPMagnitude';
    END;
$$ STABLE ROWS 100000 PARALLEL SAFE LANGUAGE plpgsql;
