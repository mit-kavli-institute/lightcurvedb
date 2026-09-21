\set ON_ERROR_STOP 0
SET search_path = spike;

\echo '=== Q3a: ordering -- detach hierarchy FIRST, then dataset. CROSS-ORBIT ROW PRESENT ==='
SELECT * FROM datasethierarchy ORDER BY source_observation_id;
BEGIN;
ALTER TABLE datasethierarchy DETACH PARTITION datasethierarchy_obs_5;
\echo '--> hierarchy_obs_5 detached OK'
ALTER TABLE dataset DETACH PARTITION dataset_obs_5;
\echo '--> dataset_obs_5 detached OK (unexpected if cross-orbit row blocks)'
ROLLBACK;

\echo ''
\echo '=== Q3b: same, but REMOVE the cross-orbit row first (simulating the intra-orbit invariant) ==='
BEGIN;
DELETE FROM datasethierarchy WHERE source_observation_id <> child_observation_id;
ALTER TABLE datasethierarchy DETACH PARTITION datasethierarchy_obs_5;
ALTER TABLE dataset DETACH PARTITION dataset_obs_5;
\echo '--> BOTH DETACHED OK with intra-orbit invariant holding'
SELECT count(*) AS dataset_rows_live FROM dataset WHERE observation_id=5;
SELECT count(*) AS detached_table_rows FROM dataset_obs_5;
\echo '--> what FK constraints survive on the detached HIERARCHY table?'
SELECT conname, convalidated, conparentid <> 0 AS has_parent, pg_get_constraintdef(oid)
  FROM pg_constraint WHERE conrelid='spike.datasethierarchy_obs_5'::regclass AND contype='f';
\echo '--> what constraints survive on the detached DATASET table? (Q9)'
SELECT conname, contype, convalidated, pg_get_constraintdef(oid)
  FROM pg_constraint WHERE conrelid='spike.dataset_obs_5'::regclass ORDER BY contype, conname;
SELECT relispartition FROM pg_class WHERE oid='spike.dataset_obs_5'::regclass;
ROLLBACK;
