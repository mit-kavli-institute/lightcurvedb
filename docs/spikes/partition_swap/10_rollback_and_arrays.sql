\set ON_ERROR_STOP 1
SET search_path = spike;
\timing on

\echo '================= ROLLBACK (mirror swap) ================='
BEGIN;
SET LOCAL lock_timeout = '5s';
LOCK TABLE dataset, datasethierarchy IN ACCESS EXCLUSIVE MODE;
ALTER TABLE datasethierarchy DETACH PARTITION datasethierarchy_obs_60_v1;
ALTER TABLE dataset          DETACH PARTITION dataset_obs_60_v1;
\echo '--- re-attach v0 dataset (NOTE: plain DETACH left no CHECK -> expect validation scan) ---'
ALTER TABLE dataset          ATTACH PARTITION dataset_obs_60_v0          FOR VALUES IN (60);
\echo '--- re-attach v0 hierarchy ---'
ALTER TABLE datasethierarchy ATTACH PARTITION datasethierarchy_obs_60_v0 FOR VALUES IN (60);
COMMIT;
\echo '--- verify old data is live again ---'
SELECT values[1] AS first_value, errors[1] AS first_error, count(*) FROM dataset WHERE observation_id=60 GROUP BY 1,2;

\echo ''
\echo '================= float8[] round-trip: NaN / Inf / empty / NULL ================='
CREATE TABLE arr_probe (id int primary key, v float8[] NOT NULL, e float8[]);
INSERT INTO arr_probe VALUES
  (1, ARRAY['NaN','Infinity','-Infinity',1.5]::float8[], NULL),
  (2, ARRAY[]::float8[], ARRAY[]::float8[]),
  (3, ARRAY[1.0,2.0]::float8[], ARRAY['NaN']::float8[]);
SELECT id, v, e,
       v[1] = 'NaN'::float8      AS nan_eq_nan,
       v[1] IS NOT DISTINCT FROM 'NaN'::float8 AS nan_not_distinct,
       cardinality(v) AS card_v, array_length(v,1) AS len_v,
       (e IS NULL) AS e_is_null
  FROM arr_probe ORDER BY id;
\echo '--- IS DISTINCT FROM on whole arrays containing NaN (used by the diff helper) ---'
SELECT ARRAY['NaN',1.0]::float8[] IS DISTINCT FROM ARRAY['NaN',1.0]::float8[] AS nan_arrays_distinct;
SELECT ARRAY[]::float8[] IS DISTINCT FROM NULL::float8[] AS empty_vs_null_distinct;
