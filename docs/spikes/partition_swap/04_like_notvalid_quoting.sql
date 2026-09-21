\set ON_ERROR_STOP 0
SET search_path = spike;

\echo '=== Q10: LIKE INCLUDING ALL from a PARTITIONED parent ==='
CREATE TABLE probe_like (LIKE dataset INCLUDING ALL);
SELECT conname, contype FROM pg_constraint WHERE conrelid='spike.probe_like'::regclass ORDER BY contype;
SELECT indexrelid::regclass AS idx, indisprimary, indisunique FROM pg_index WHERE indrelid='spike.probe_like'::regclass;
SELECT count(*) AS fks_copied FROM pg_constraint WHERE conrelid='spike.probe_like'::regclass AND contype='f';
SELECT relkind AS kind_r_means_plain_heap FROM pg_class WHERE oid='spike.probe_like'::regclass;
SELECT attname, attnotnull FROM pg_attribute WHERE attrelid='spike.probe_like'::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum;
DROP TABLE probe_like;

\echo ''
\echo '=== Q10b: LIKE ... INCLUDING ALL EXCLUDING INDEXES ==='
CREATE TABLE probe_noidx (LIKE dataset INCLUDING ALL EXCLUDING INDEXES,
                          CONSTRAINT probe_noidx_partcheck CHECK (observation_id = 99));
SELECT conname, contype FROM pg_constraint WHERE conrelid='spike.probe_noidx'::regclass ORDER BY contype;
SELECT count(*) AS indexes FROM pg_index WHERE indrelid='spike.probe_noidx'::regclass;
DROP TABLE probe_noidx;

\echo ''
\echo '=== Q11: unquoted values column ==='
SELECT observation_id, values FROM dataset LIMIT 0;
\echo '--> unquoted SELECT of values parsed OK'

\echo ''
\echo '=== Q7: ADD FOREIGN KEY ... NOT VALID on a PARTITIONED table? ==='
ALTER TABLE datasethierarchy DROP CONSTRAINT fk_datasethierarchy_child;
ALTER TABLE datasethierarchy ADD CONSTRAINT fk_datasethierarchy_child FOREIGN KEY
  (child_observation_id, child_target_id, child_photometric_method_id, child_processing_method_id)
  REFERENCES dataset (observation_id, target_id, photometric_method_id, processing_method_id)
  ON DELETE CASCADE NOT VALID;
\echo '--> if you see this, NOT VALID was ACCEPTED on a partitioned table'
-- restore either way
ALTER TABLE datasethierarchy DROP CONSTRAINT IF EXISTS fk_datasethierarchy_child;
ALTER TABLE datasethierarchy ADD CONSTRAINT fk_datasethierarchy_child FOREIGN KEY
  (child_observation_id, child_target_id, child_photometric_method_id, child_processing_method_id)
  REFERENCES dataset (observation_id, target_id, photometric_method_id, processing_method_id)
  ON DELETE CASCADE;
SELECT conname, convalidated FROM pg_constraint WHERE conrelid='spike.datasethierarchy'::regclass AND contype='f';
