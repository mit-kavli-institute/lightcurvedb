\set ON_ERROR_STOP 0
SET search_path = spike;
\timing on

\echo '=== Q8a: DETACH CONCURRENTLY inside a transaction block ==='
BEGIN;
ALTER TABLE plain DETACH PARTITION plain_51 CONCURRENTLY;
ROLLBACK;

\echo ''
\echo '=== Q8b: DETACH CONCURRENTLY in autocommit, NO default partition ==='
ALTER TABLE plain DETACH PARTITION plain_51 CONCURRENTLY;
SELECT relispartition FROM pg_class WHERE oid='spike.plain_51'::regclass;
\echo '--- what does CONCURRENTLY leave behind? (Q9) ---'
SELECT conname, contype, pg_get_constraintdef(oid) FROM pg_constraint
 WHERE conrelid='spike.plain_51'::regclass AND contype='c';

\echo ''
\echo '=== Q6: ATTACH cost with a DEFAULT partition present ==='
CREATE TABLE plain_default PARTITION OF plain DEFAULT;
INSERT INTO plain_default SELECT 999, t, 0, 0, ARRAY[1.0], NULL FROM generate_series(1,400000) t;
ANALYZE plain_default;
CREATE TABLE plain_52 (LIKE plain INCLUDING ALL EXCLUDING INDEXES,
                       CONSTRAINT plain_52_partcheck CHECK (observation_id = 52));
INSERT INTO plain_52 SELECT 52, t, 0, 0, ARRAY[1.0], NULL FROM generate_series(1,1000) t;
ALTER TABLE plain_52 ADD CONSTRAINT plain_52_pkey PRIMARY KEY (observation_id,target_id,photometric_method_id,processing_method_id);
CREATE INDEX plain_52_target_idx ON plain_52 (target_id);
ANALYZE plain_52;
\echo '>>> TIMING ATTACH with 400k-row DEFAULT partition present (has matching CHECK):'
ALTER TABLE plain ATTACH PARTITION plain_52 FOR VALUES IN (52);

\echo ''
\echo '=== Q6b: ATTACH when the DEFAULT partition HOLDS a conflicting row ==='
INSERT INTO plain_default SELECT 53, 1, 0, 0, ARRAY[1.0], NULL;
CREATE TABLE plain_53 (LIKE plain INCLUDING ALL EXCLUDING INDEXES,
                       CONSTRAINT plain_53_partcheck CHECK (observation_id = 53));
ALTER TABLE plain_53 ADD CONSTRAINT plain_53_pkey PRIMARY KEY (observation_id,target_id,photometric_method_id,processing_method_id);
CREATE INDEX plain_53_target_idx ON plain_53 (target_id);
ALTER TABLE plain ATTACH PARTITION plain_53 FOR VALUES IN (53);
\echo '--> if you see this without error, attach succeeded despite conflicting default row'

\echo ''
\echo '=== Q8c: DETACH CONCURRENTLY WITH a default partition present ==='
ALTER TABLE plain DETACH PARTITION plain_50 CONCURRENTLY;
\echo '--> if you see this, CONCURRENTLY was allowed with a default partition'
