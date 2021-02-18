CREATE OR REPLACE FUNCTION
dump_home_partition_of(
    _lightcurve_id bigint
)
RETURNS SETOF lightpoints AS $$
DECLARE
    partition_id bigint;
    partition_name varchar;
BEGIN
    partition_id := (_lightcurve_id / 1000) * 1000;
    partition_name := 'partitions.lightpoints_' || partition_id::text || '_' || (partition_id + 1000)::text;

    RETURN QUERY EXECUTE FORMAT('SELECT * FROM %%s ', partition_name);
END;
$$ STABLE ROWS 10000000 PARALLEL SAFE LANGUAGE plpgsql;
