# Partition-swap spikes (PostgreSQL 14)

Throwaway scripts that settle the PostgreSQL semantics the lightcurve
replacement design depends on. Results are summarised in
[`../../PARTITION_REPLACEMENT_REQUIREMENTS.md`](../../PARTITION_REPLACEMENT_REQUIREMENTS.md) §8.

They build a `spike` schema reproducing the **exact rendered DDL** of `dataset`
and `datasethierarchy`, including the `int4`/`int8` `target_id` mismatch, so the
findings transfer directly.

## Running

```bash
docker compose up -d db
for f in docs/spikes/partition_swap/*.sql; do
  echo "== $f"
  docker compose exec -T db psql -U postgres -d postgres -f - < "$f"
done
python docs/spikes/partition_swap/copy_test.py
```

Run them **in numeric order** — each builds on the previous one's state. Several
deliberately provoke errors and set `ON_ERROR_STOP 0`; the errors *are* the
results. `copy_test.py` needs `psycopg[binary]` and `numpy`, and honours
`LCDB_SPIKE_DSN` (or `POSTGRES_USER` / `POSTGRES_PASSWORD` /
`POSTGRES_HOST_LOCAL` / `POSTGRES_PORT`).

This schema is disposable: `DROP SCHEMA spike CASCADE;`

## What each file answers

| File | Question |
|---|---|
| `00_setup.sql` | Builds the spike schema |
| `01_fk_catalog_growth.sql` | Q1 — is FK catalog growth linear or quadratic in partition count? |
| `02_detach_referenced.sql` | Q2 — does detaching a referenced partition dangle or block? |
| `03_detach_ordering.sql` | Q3, Q9 — detach ordering, cross-orbit blocking, what survives detach |
| `04_like_notvalid_quoting.sql` | Q10, Q11, Q7 — `LIKE INCLUDING ALL`, unquoted `values`, `NOT VALID` |
| `05_check_skips_scan.sql` | Q4 — does a matching `CHECK` skip the attach validation scan? |
| `06_fk_adoption.sql` | Q5 — is a pre-created validated FK adopted on attach? |
| `07_default_partition_and_concurrently.sql` | Q6, Q8 — DEFAULT partition cost, `DETACH CONCURRENTLY` limits |
| `08_swap_chicken_and_egg.sql` | Why a pre-created hierarchy FK **blocks** the swap (§8.1) |
| `09_swap_correct.sql` | Q12 — the correct swap, timed per statement |
| `10_rollback_and_arrays.sql` | Rollback swap; `float8[]` NaN/Inf/empty/NULL semantics |
| `11_target_id_defect_and_invariant.sql` | Proves the `int4` `target_id` defect; validates the intra-orbit CHECK |
| `copy_test.py` | Binary `COPY` round-trip fidelity, differential vs `executemany`, throughput |
