\set ON_ERROR_STOP 1
DROP SCHEMA IF EXISTS spike CASCADE;
CREATE SCHEMA spike;
SET search_path = spike;

CREATE TABLE observation        (id int    PRIMARY KEY);
CREATE TABLE target             (id bigint PRIMARY KEY);
CREATE TABLE photometric_source (id int    PRIMARY KEY);
CREATE TABLE processing_method  (id int    PRIMARY KEY);
INSERT INTO observation        SELECT generate_series(1, 60);
INSERT INTO target             SELECT generate_series(1, 400000);
INSERT INTO photometric_source VALUES (0);
INSERT INTO processing_method  VALUES (0);

-- EXACTLY what SQLAlchemy renders today (int4/int8 mismatch included)
CREATE TABLE dataset (
    observation_id        integer NOT NULL REFERENCES observation(id)        ON DELETE CASCADE,
    target_id             bigint  NOT NULL REFERENCES target(id)             ON DELETE CASCADE,
    photometric_method_id integer NOT NULL REFERENCES photometric_source(id) ON DELETE RESTRICT,
    processing_method_id  integer NOT NULL REFERENCES processing_method(id)  ON DELETE RESTRICT,
    values float8[] NOT NULL,
    errors float8[],
    CONSTRAINT pk_dataset PRIMARY KEY
        (observation_id, target_id, photometric_method_id, processing_method_id)
) PARTITION BY LIST (observation_id);
CREATE INDEX ix_dataset_target_id ON dataset (target_id);

CREATE TABLE datasethierarchy (
    source_observation_id        integer NOT NULL,
    source_target_id             integer NOT NULL,
    source_photometric_method_id integer NOT NULL,
    source_processing_method_id  integer NOT NULL,
    child_observation_id         integer NOT NULL,
    child_target_id              integer NOT NULL,
    child_photometric_method_id  integer NOT NULL,
    child_processing_method_id   integer NOT NULL,
    CONSTRAINT pk_datasethierarchy PRIMARY KEY (
        source_observation_id, source_target_id,
        source_photometric_method_id, source_processing_method_id,
        child_observation_id, child_target_id,
        child_photometric_method_id, child_processing_method_id),
    CONSTRAINT fk_datasethierarchy_source FOREIGN KEY
        (source_observation_id, source_target_id,
         source_photometric_method_id, source_processing_method_id)
        REFERENCES dataset (observation_id, target_id,
                            photometric_method_id, processing_method_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_datasethierarchy_child FOREIGN KEY
        (child_observation_id, child_target_id,
         child_photometric_method_id, child_processing_method_id)
        REFERENCES dataset (observation_id, target_id,
                            photometric_method_id, processing_method_id)
        ON DELETE CASCADE
) PARTITION BY LIST (source_observation_id);
CREATE INDEX ix_datasethierarchy_source ON datasethierarchy
    (source_observation_id, source_target_id,
     source_photometric_method_id, source_processing_method_id);
CREATE INDEX ix_datasethierarchy_child ON datasethierarchy
    (child_observation_id, child_target_id,
     child_photometric_method_id, child_processing_method_id);

CREATE FUNCTION spike.counts() RETURNS TABLE(fk_constraints bigint, internal_triggers bigint)
LANGUAGE sql AS $$
  SELECT (SELECT count(*) FROM pg_constraint c
            JOIN pg_class r ON r.oid=c.conrelid
            JOIN pg_namespace n ON n.oid=r.relnamespace
           WHERE n.nspname='spike' AND c.contype='f'),
         (SELECT count(*) FROM pg_trigger t
            JOIN pg_class r ON r.oid=t.tgrelid
            JOIN pg_namespace n ON n.oid=r.relnamespace
           WHERE n.nspname='spike' AND t.tgisinternal);
$$;

CREATE FUNCTION spike.mk(obs int) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE format('CREATE TABLE spike.dataset_obs_%s PARTITION OF spike.dataset FOR VALUES IN (%s)', obs, obs);
  EXECUTE format('CREATE TABLE spike.datasethierarchy_obs_%s PARTITION OF spike.datasethierarchy FOR VALUES IN (%s)', obs, obs);
END $$;
