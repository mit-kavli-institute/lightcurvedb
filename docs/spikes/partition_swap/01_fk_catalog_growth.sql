\set ON_ERROR_STOP 1
SET search_path = spike;
\timing on
SELECT 0 AS n_orbit_pairs, * FROM spike.counts();
SELECT spike.mk(g) FROM generate_series(1,5) g;
SELECT 5 AS n_orbit_pairs, * FROM spike.counts();
SELECT spike.mk(g) FROM generate_series(6,10) g;
SELECT 10 AS n_orbit_pairs, * FROM spike.counts();
SELECT spike.mk(g) FROM generate_series(11,20) g;
SELECT 20 AS n_orbit_pairs, * FROM spike.counts();
SELECT spike.mk(g) FROM generate_series(21,40) g;
SELECT 40 AS n_orbit_pairs, * FROM spike.counts();
