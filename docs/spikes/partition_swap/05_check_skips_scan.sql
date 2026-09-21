\set ON_ERROR_STOP 1
SET search_path = spike;
\timing on

-- FK-free clone so we measure ONLY the partition-constraint validation scan
CREATE TABLE plain (
    observation_id integer NOT NULL,
    target_id bigint NOT NULL,
    photometric_method_id integer NOT NULL,
    processing_method_id integer NOT NULL,
    values float8[] NOT NULL,
    errors float8[],
    CONSTRAINT pk_plain PRIMARY KEY (observation_id,target_id,photometric_method_id,processing_method_id)
) PARTITION BY LIST (observation_id);
CREATE INDEX ix_plain_target_id ON plain (target_id);

\echo '--- build WITH check (obs 50) ---'
CREATE TABLE plain_50 (LIKE plain INCLUDING ALL EXCLUDING INDEXES,
                       CONSTRAINT plain_50_partcheck CHECK (observation_id = 50));
INSERT INTO plain_50 SELECT 50, t, 0, 0, ARRAY[1.0,2.0,3.0], NULL FROM generate_series(1,300000) t;
ALTER TABLE plain_50 ADD CONSTRAINT plain_50_pkey PRIMARY KEY (observation_id,target_id,photometric_method_id,processing_method_id);
CREATE INDEX plain_50_target_idx ON plain_50 (target_id);
ANALYZE plain_50;

\echo '--- build WITHOUT check (obs 51) ---'
CREATE TABLE plain_51 (LIKE plain INCLUDING ALL EXCLUDING INDEXES);
INSERT INTO plain_51 SELECT 51, t, 0, 0, ARRAY[1.0,2.0,3.0], NULL FROM generate_series(1,300000) t;
ALTER TABLE plain_51 ADD CONSTRAINT plain_51_pkey PRIMARY KEY (observation_id,target_id,photometric_method_id,processing_method_id);
CREATE INDEX plain_51_target_idx ON plain_51 (target_id);
ANALYZE plain_51;

SELECT pg_stat_reset_single_table_counters('spike.plain_50'::regclass);
SELECT pg_stat_reset_single_table_counters('spike.plain_51'::regclass);
SELECT pg_sleep(1.5);

\echo '=== Q4: ATTACH WITH matching CHECK (expect: fast, no seq scan) ==='
ALTER TABLE plain ATTACH PARTITION plain_50 FOR VALUES IN (50);

\echo '=== Q4: ATTACH WITHOUT CHECK (expect: slower, full seq scan) ==='
ALTER TABLE plain ATTACH PARTITION plain_51 FOR VALUES IN (51);

SELECT pg_sleep(1.5);
\echo '--- seq_scan counters (50=with check, 51=without) ---'
SELECT relname, seq_scan, seq_tup_read FROM pg_stat_all_tables
 WHERE relid IN ('spike.plain_50'::regclass,'spike.plain_51'::regclass) ORDER BY relname;

\echo '--- Q10c: were the parent indexes ADOPTED as partitioned-index children? ---'
SELECT ci.relname AS child_index, cp.relname AS parent_index
  FROM pg_inherits ii
  JOIN pg_class ci ON ci.oid=ii.inhrelid AND ci.relkind='i'
  JOIN pg_class cp ON cp.oid=ii.inhparent AND cp.relkind='I'
 WHERE ci.relname LIKE 'plain_5%' ORDER BY ci.relname;
SELECT indexrelid::regclass, indisvalid FROM pg_index WHERE indrelid='spike.plain_50'::regclass;
