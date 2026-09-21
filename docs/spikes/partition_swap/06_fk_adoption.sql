\set ON_ERROR_STOP 1
SET search_path = spike;
\timing on

-- real dataset partitions for obs 50/51 so hierarchy rows have referents
CREATE TABLE dataset_obs_50 PARTITION OF dataset FOR VALUES IN (50);
CREATE TABLE dataset_obs_51 PARTITION OF dataset FOR VALUES IN (51);
INSERT INTO dataset SELECT 50, t, 0, 0, ARRAY[1.0,2.0], NULL FROM generate_series(1,300000) t;
INSERT INTO dataset SELECT 51, t, 0, 0, ARRAY[1.0,2.0], NULL FROM generate_series(1,300000) t;
ANALYZE dataset;

\echo ''
\echo '=== Q5a: attach hierarchy partition with NO pre-created FK ==='
CREATE TABLE dh_stage_50 (LIKE datasethierarchy INCLUDING ALL EXCLUDING INDEXES,
                          CONSTRAINT dh_stage_50_partcheck CHECK (source_observation_id = 50));
INSERT INTO dh_stage_50 SELECT 50,t,0,0, 50,t,0,0 FROM generate_series(1,300000) t;
ALTER TABLE dh_stage_50 ADD CONSTRAINT dh_stage_50_pkey PRIMARY KEY (
  source_observation_id, source_target_id, source_photometric_method_id, source_processing_method_id,
  child_observation_id, child_target_id, child_photometric_method_id, child_processing_method_id);
CREATE INDEX dh_stage_50_src_idx   ON dh_stage_50 (source_observation_id, source_target_id, source_photometric_method_id, source_processing_method_id);
CREATE INDEX dh_stage_50_child_idx ON dh_stage_50 (child_observation_id, child_target_id, child_photometric_method_id, child_processing_method_id);
ANALYZE dh_stage_50;
\echo '>>> TIMING ATTACH (no pre-created FK):'
ALTER TABLE datasethierarchy ATTACH PARTITION dh_stage_50 FOR VALUES IN (50);

\echo ''
\echo '=== Q5b: attach hierarchy partition WITH pre-created, pre-VALIDATED FKs ==='
CREATE TABLE dh_stage_51 (LIKE datasethierarchy INCLUDING ALL EXCLUDING INDEXES,
                          CONSTRAINT dh_stage_51_partcheck CHECK (source_observation_id = 51));
INSERT INTO dh_stage_51 SELECT 51,t,0,0, 51,t,0,0 FROM generate_series(1,300000) t;
ALTER TABLE dh_stage_51 ADD CONSTRAINT dh_stage_51_pkey PRIMARY KEY (
  source_observation_id, source_target_id, source_photometric_method_id, source_processing_method_id,
  child_observation_id, child_target_id, child_photometric_method_id, child_processing_method_id);
CREATE INDEX dh_stage_51_src_idx   ON dh_stage_51 (source_observation_id, source_target_id, source_photometric_method_id, source_processing_method_id);
CREATE INDEX dh_stage_51_child_idx ON dh_stage_51 (child_observation_id, child_target_id, child_photometric_method_id, child_processing_method_id);
\echo '--- pre-create the two FKs OUTSIDE the swap window ---'
ALTER TABLE dh_stage_51 ADD CONSTRAINT fk_src_pre FOREIGN KEY
  (source_observation_id, source_target_id, source_photometric_method_id, source_processing_method_id)
  REFERENCES dataset (observation_id, target_id, photometric_method_id, processing_method_id) ON DELETE CASCADE;
ALTER TABLE dh_stage_51 ADD CONSTRAINT fk_child_pre FOREIGN KEY
  (child_observation_id, child_target_id, child_photometric_method_id, child_processing_method_id)
  REFERENCES dataset (observation_id, target_id, photometric_method_id, processing_method_id) ON DELETE CASCADE;
ANALYZE dh_stage_51;
SELECT conname, convalidated, conparentid FROM pg_constraint WHERE conrelid='spike.dh_stage_51'::regclass AND contype='f';
\echo '>>> TIMING ATTACH (with pre-created validated FKs):'
ALTER TABLE datasethierarchy ATTACH PARTITION dh_stage_51 FOR VALUES IN (51);

\echo ''
\echo '--- were the pre-created FKs ADOPTED (conparentid set) or duplicated? ---'
SELECT conname, convalidated, conparentid <> 0 AS adopted FROM pg_constraint
 WHERE conrelid='spike.dh_stage_51'::regclass AND contype='f' ORDER BY conname;
\echo '--- how many FKs on the no-pre-FK partition? ---'
SELECT conname, convalidated, conparentid <> 0 AS adopted FROM pg_constraint
 WHERE conrelid='spike.dh_stage_50'::regclass AND contype='f' ORDER BY conname;
