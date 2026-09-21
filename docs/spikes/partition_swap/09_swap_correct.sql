\set ON_ERROR_STOP 1
SET search_path = spike;
\timing on

\echo '--- remove the counter-productive pre-created hierarchy FK ---'
ALTER TABLE datasethierarchy_obs_60_v1 DROP CONSTRAINT dh60v1_src_fk;

\echo ''
\echo '================= THE SWAP TRANSACTION (correct form) ================='
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';
LOCK TABLE dataset, datasethierarchy IN ACCESS EXCLUSIVE MODE;
\echo '--- 1. detach referencing side ---'
ALTER TABLE datasethierarchy DETACH PARTITION datasethierarchy_obs_60_v0;
\echo '--- 2. detach referenced side ---'
ALTER TABLE dataset          DETACH PARTITION dataset_obs_60_v0;
\echo '--- 3. attach new referenced side (pre-created FKs adopted) ---'
ALTER TABLE dataset          ATTACH PARTITION dataset_obs_60_v1          FOR VALUES IN (60);
\echo '--- 4. attach new referencing side (FK cloned + validated here) ---'
ALTER TABLE datasethierarchy ATTACH PARTITION datasethierarchy_obs_60_v1 FOR VALUES IN (60);
COMMIT;
\echo '================= COMMITTED ================='

\echo ''
\echo '--- verification: new data live? ---'
SELECT values[1] AS first_value, errors[1] AS first_error, count(*) FROM dataset WHERE observation_id=60 GROUP BY 1,2;
SELECT count(*) AS hier_rows_live FROM datasethierarchy WHERE source_observation_id=60;
\echo '--- retired tables still intact and detached? ---'
SELECT relname, relispartition FROM pg_class WHERE relname IN ('dataset_obs_60_v0','datasethierarchy_obs_60_v0');
SELECT count(*) AS retired_ds_rows FROM dataset_obs_60_v0;
SELECT count(*) AS retired_hier_rows FROM datasethierarchy_obs_60_v0;
\echo '--- did the v1 dataset FKs get adopted? ---'
SELECT conname, conparentid <> 0 AS adopted FROM pg_constraint
 WHERE conrelid='spike.dataset_obs_60_v1'::regclass AND contype='f' AND conname LIKE 'ds60v1%' ORDER BY conname;
