\set ON_ERROR_STOP 0
SET search_path = spike;

\echo '=== target_id int4/int8 defect: does it actually bite? ==='
-- a TIC-scale target id, well above 2^31 (2147483647)
INSERT INTO target VALUES (10005000540);
CREATE TABLE dataset_obs_59 PARTITION OF dataset FOR VALUES IN (59);
INSERT INTO dataset VALUES (59, 10005000540, 0, 0, ARRAY[1.0], NULL);
\echo '--> dataset row with target_id=10005000540 inserted OK (BIGINT)'
SELECT observation_id, target_id FROM dataset WHERE observation_id=59;

CREATE TABLE datasethierarchy_obs_59 PARTITION OF datasethierarchy FOR VALUES IN (59);
\echo '--> now try to record lineage for that same target:'
INSERT INTO datasethierarchy VALUES (59, 10005000540, 0, 0, 59, 10005000540, 0, 0);
\echo '--> if you see no error above, the int4 column accepted it'

\echo ''
\echo '=== and the fix: widen to bigint ==='
ALTER TABLE datasethierarchy
  ALTER COLUMN source_target_id TYPE bigint,
  ALTER COLUMN child_target_id  TYPE bigint;
\echo '--> widened; retry the lineage insert:'
INSERT INTO datasethierarchy VALUES (59, 10005000540, 0, 0, 59, 10005000540, 0, 0);
SELECT source_observation_id, source_target_id FROM datasethierarchy WHERE source_observation_id=59;

\echo ''
\echo '=== intra-orbit invariant: does the CHECK hold on real data? ==='
ALTER TABLE datasethierarchy ADD CONSTRAINT ck_intra_orbit_lineage
  CHECK (source_observation_id = child_observation_id) NOT VALID;
ALTER TABLE datasethierarchy VALIDATE CONSTRAINT ck_intra_orbit_lineage;
\echo '--> VALIDATE result above (we still have the cross-orbit row 7->5 from Q2)'
SELECT count(*) AS cross_orbit_rows FROM datasethierarchy WHERE source_observation_id <> child_observation_id;
