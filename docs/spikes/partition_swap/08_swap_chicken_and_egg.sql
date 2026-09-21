\set ON_ERROR_STOP 0
SET search_path = spike;
\timing on

-- ---------- live v0 pair for obs 60 ----------
CREATE TABLE dataset_obs_60_v0 PARTITION OF dataset FOR VALUES IN (60);
INSERT INTO dataset SELECT 60, t, 0, 0, ARRAY[1.0,2.0], NULL FROM generate_series(1,300000) t;
CREATE TABLE datasethierarchy_obs_60_v0 PARTITION OF datasethierarchy FOR VALUES IN (60);
INSERT INTO datasethierarchy SELECT 60,t,0,0, 60,t,0,0 FROM generate_series(1,300000) t;
ANALYZE dataset_obs_60_v0; ANALYZE datasethierarchy_obs_60_v0;

-- ---------- staged v1 pair, built fully OUTSIDE the swap window ----------
CREATE TABLE dataset_obs_60_v1 (LIKE dataset INCLUDING ALL EXCLUDING INDEXES,
    CONSTRAINT dataset_obs_60_v1_partcheck CHECK (observation_id = 60));
INSERT INTO dataset_obs_60_v1 SELECT 60, t, 0, 0, ARRAY[9.9,8.8], ARRAY[0.1,0.2] FROM generate_series(1,300000) t;
ALTER TABLE dataset_obs_60_v1 ADD CONSTRAINT dataset_obs_60_v1_pkey PRIMARY KEY (observation_id,target_id,photometric_method_id,processing_method_id);
CREATE INDEX dataset_obs_60_v1_target_idx ON dataset_obs_60_v1 (target_id);
\echo '--- pre-create dataset v1 outbound FKs (references live non-partitioned tables) ---'
ALTER TABLE dataset_obs_60_v1 ADD CONSTRAINT ds60v1_obs_fk  FOREIGN KEY (observation_id) REFERENCES observation(id) ON DELETE CASCADE;
ALTER TABLE dataset_obs_60_v1 ADD CONSTRAINT ds60v1_tgt_fk  FOREIGN KEY (target_id) REFERENCES target(id) ON DELETE CASCADE;
ALTER TABLE dataset_obs_60_v1 ADD CONSTRAINT ds60v1_phot_fk FOREIGN KEY (photometric_method_id) REFERENCES photometric_source(id) ON DELETE RESTRICT;
ALTER TABLE dataset_obs_60_v1 ADD CONSTRAINT ds60v1_proc_fk FOREIGN KEY (processing_method_id) REFERENCES processing_method(id) ON DELETE RESTRICT;
ANALYZE dataset_obs_60_v1;

CREATE TABLE datasethierarchy_obs_60_v1 (LIKE datasethierarchy INCLUDING ALL EXCLUDING INDEXES,
    CONSTRAINT dh_obs_60_v1_partcheck CHECK (source_observation_id = 60));
INSERT INTO datasethierarchy_obs_60_v1 SELECT 60,t,0,0, 60,t,0,0 FROM generate_series(1,300000) t;
ALTER TABLE datasethierarchy_obs_60_v1 ADD CONSTRAINT dh_obs_60_v1_pkey PRIMARY KEY (
  source_observation_id,source_target_id,source_photometric_method_id,source_processing_method_id,
  child_observation_id,child_target_id,child_photometric_method_id,child_processing_method_id);
CREATE INDEX dh_obs_60_v1_src_idx   ON datasethierarchy_obs_60_v1 (source_observation_id,source_target_id,source_photometric_method_id,source_processing_method_id);
CREATE INDEX dh_obs_60_v1_child_idx ON datasethierarchy_obs_60_v1 (child_observation_id,child_target_id,child_photometric_method_id,child_processing_method_id);
ANALYZE datasethierarchy_obs_60_v1;

\echo ''
\echo '--- CHICKEN-AND-EGG CHECK: can we pre-validate the staged hierarchy FK now? ---'
ALTER TABLE datasethierarchy_obs_60_v1 ADD CONSTRAINT dh60v1_src_fk FOREIGN KEY
  (source_observation_id,source_target_id,source_photometric_method_id,source_processing_method_id)
  REFERENCES dataset (observation_id,target_id,photometric_method_id,processing_method_id) ON DELETE CASCADE;
\echo '--> if no error above, pre-validation SUCCEEDED against still-live v0 rows'

\echo ''
\echo '================= THE SWAP TRANSACTION ================='
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';
LOCK TABLE dataset, datasethierarchy IN ACCESS EXCLUSIVE MODE;
ALTER TABLE datasethierarchy DETACH PARTITION datasethierarchy_obs_60_v0;
ALTER TABLE dataset          DETACH PARTITION dataset_obs_60_v0;
ALTER TABLE dataset          ATTACH PARTITION dataset_obs_60_v1          FOR VALUES IN (60);
ALTER TABLE datasethierarchy ATTACH PARTITION datasethierarchy_obs_60_v1 FOR VALUES IN (60);
COMMIT;
\echo '================= SWAP COMMITTED ================='

SELECT values[1] AS first_value, count(*) FROM dataset WHERE observation_id=60 GROUP BY 1;
SELECT count(*) AS hier_rows FROM datasethierarchy WHERE source_observation_id=60;
SELECT count(*) AS retired_ds_rows FROM dataset_obs_60_v0;
