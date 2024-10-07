CREATE OR REPLACE FUNCTION
get_lightcurve_lengths(
    _tic_id bigint
)
RETURNS TABLE(lightcurve_id bigint, cadence_count bigint) AS $$
DECLARE
    ids bigint[];
BEGIN
    SELECT array_agg(id) INTO STRICT ids FROM lightcurves WHERE tic_id = _tic_id;
    RETURN QUERY SELECT lp.lightcurve_id, COUNT(DISTINCT lp.cadence) FROM lightpoints lp WHERE lp.lightcurve_id = ANY(ids) GROUP BY lp.lightcurve_id;
END;
$$ STABLE ROWS 11 PARALLEL SAFE LANGUAGE plpgsql;
