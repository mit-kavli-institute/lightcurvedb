\set ON_ERROR_STOP 0
SET search_path = spike;

INSERT INTO dataset SELECT 5, t, 0, 0, ARRAY[1.0,2.0], NULL FROM generate_series(1,100) t;
INSERT INTO dataset SELECT 7, t, 0, 0, ARRAY[1.0,2.0], NULL FROM generate_series(1,100) t;
INSERT INTO datasethierarchy VALUES (5,1,0,0, 5,2,0,0);   -- intra-orbit
INSERT INTO datasethierarchy VALUES (7,1,0,0, 5,3,0,0);   -- CROSS-orbit

\echo ''
\echo '=== Q2: DETACH the REFERENCED-side partition while rows reference it ==='
BEGIN;
ALTER TABLE dataset DETACH PARTITION dataset_obs_5;
\echo '--> if you see this, DETACH WAS ALLOWED'
SELECT count(*) AS hier_rows_still_visible FROM datasethierarchy;
SELECT count(*) AS dangling_source FROM datasethierarchy h LEFT JOIN dataset d
   ON d.observation_id=h.source_observation_id AND d.target_id=h.source_target_id
  AND d.photometric_method_id=h.source_photometric_method_id
  AND d.processing_method_id=h.source_processing_method_id
 WHERE d.observation_id IS NULL;
SELECT count(*) AS dangling_child FROM datasethierarchy h LEFT JOIN dataset d
   ON d.observation_id=h.child_observation_id AND d.target_id=h.child_target_id
  AND d.photometric_method_id=h.child_photometric_method_id
  AND d.processing_method_id=h.child_processing_method_id
 WHERE d.observation_id IS NULL;
\echo '--> can we still INSERT a hierarchy row pointing at the detached data?'
INSERT INTO datasethierarchy VALUES (5,4,0,0, 5,5,0,0);
ROLLBACK;

\echo ''
\echo '=== Q2b: does re-ATTACH restore cleanly? ==='
BEGIN;
ALTER TABLE dataset DETACH PARTITION dataset_obs_5;
ALTER TABLE dataset ATTACH PARTITION dataset_obs_5 FOR VALUES IN (5);
SELECT count(*) AS rows_back FROM dataset WHERE observation_id=5;
COMMIT;

\echo ''
\echo '=== Q2c: DROP the detached partition entirely -- are hier rows cascaded? ==='
BEGIN;
ALTER TABLE dataset DETACH PARTITION dataset_obs_5;
DROP TABLE dataset_obs_5;
SELECT count(*) AS hier_rows_after_drop FROM datasethierarchy;
ROLLBACK;
