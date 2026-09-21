# Requirements: En-Masse Lightcurve Replacement via Partition Swap

> **Status:** DRAFT REQUIREMENTS — input for a later implementation session, not
> a plan to execute verbatim.
>
> **§8 is blocking.** Spikes Q1, Q2 and Q5 must be answered before any
> production code is written. Q1 can invalidate the composite foreign keys
> entirely; Q5 determines whether the swap window is milliseconds or minutes.

---

## 1. Context

Several TESS orbits have lightcurves that were produced incorrectly and must be
regenerated and replaced wholesale. The replacement cannot be a destructive
in-place overwrite:

- **Both the broken and the corrected lightcurves must coexist** for a review
  period so the team can compare them and sign off.
- Only after sign-off are the broken lightcurves removed.
- Volume is large — order 10⁵–10⁶ `dataset` rows per orbit, each holding
  `float8[]` arrays of 10³–10⁴ elements (tens of GB per orbit). Row-by-row
  `UPDATE`/`DELETE` would bloat the heap, trigger a VACUUM storm, and take hours.

`dataset` is already LIST-partitioned by `observation_id`, and one orbit maps to
exactly one `Observation` row. Partition-level operations
(`ATTACH`/`DETACH`/`DROP`) are therefore the natural unit of replacement: O(1)
catalog operations rather than O(rows) data operations.

**Intended outcome:** a supported, tested Python API in `lightcurvedb` for
staging, validating, promoting, rolling back and retiring per-observation
partitions of `dataset` and `datasethierarchy`, plus a provenance record of
which revision is live and why.

### Decisions already made — do not relitigate

| Decision | Choice |
|---|---|
| Visibility during review | **Old stays live; new is staged detached.** Swap on approval. |
| Tables swapped together | **`dataset` + `datasethierarchy`.** `target_specific_time` out of scope. |
| Schema delivery | **Idempotent bootstrap helpers.** No Alembic. |
| API surface | **Python library only.** No CLI. |
| Cross-orbit lineage | **Does not occur.** Enforce `source_observation_id = child_observation_id`. |
| `target_id` type defect | **Fix as a prerequisite in this branch.** |

---

## 2. Glossary

| Term | Meaning |
|---|---|
| **Orbit** | One `Observation` row. There is deliberately no `Orbit` model in `src/` — it was removed (`18840ea`, `87a53c9`); downstream projects subclass `Observation` (docstring example at `src/lightcurvedb/models/observation.py:52-58`). Identify orbits by `observation.id`. |
| **Logical partition slot** | The pair `(base_table, observation_id)`. |
| **Physical relation** | An actual table, e.g. `dataset_obs_5_v4`. Many exist over a slot's lifetime; at most one is attached. |
| **Revision** | Monotonic integer per slot. Legacy `dataset_obs_5` (the CHANGELOG convention) is revision 0. |
| **Live / active** | The physical relation currently `ATTACH`ed. Visible to ordinary `session.query(DataSet)`. |
| **Staged** | Standalone table holding rebuilt data, structurally attachable but **not attached**. Invisible to ordinary queries. |
| **Retired** | Previously-live relation after detach, kept for rollback until dropped. |
| **Campaign** | One replacement effort spanning several orbits, tracked as a unit. |

---

## 3. Current state (verified against `master` @ 3.1.0)

**`CLAUDE.md` is substantially stale** — it describes a `cli/`, `managers/`,
`util/sql.py`, plotting utilities and a `version-2` branch, none of which exist.
Do not trust it. The shipped package is ~2,350 lines across 20 files.

**What exists**

| Table | Partition key | Declared at |
|---|---|---|
| `dataset` | `LIST (observation_id)` | `src/lightcurvedb/models/dataset.py:402` |
| `datasethierarchy` | `LIST (source_observation_id)` | `src/lightcurvedb/models/dataset.py:271` |
| `target_specific_time` | `LIST (observation_id)` | `src/lightcurvedb/models/observation.py:224` |

- `DataSet` composite PK `pk_dataset (observation_id, target_id,
  photometric_method_id, processing_method_id)` — partition key first by design.
- `DataSetHierarchy`: 8-column PK, **two 4-column composite FKs into `dataset`**
  (`fk_datasethierarchy_source`, `fk_datasethierarchy_child`), both
  `ON DELETE CASCADE`, plus two 4-column covering indexes.
- Parent index set the staging tables must reproduce:

  | Table | Index | Kind |
  |---|---|---|
  | `dataset` | `pk_dataset` (4 cols) | PRIMARY KEY |
  | `dataset` | `ix_dataset_target_id (target_id)` | btree |
  | `datasethierarchy` | `pk_datasethierarchy` (8 cols) | PRIMARY KEY |
  | `datasethierarchy` | `ix_datasethierarchy_source` (4 cols) | btree |
  | `datasethierarchy` | `ix_datasethierarchy_child` (4 cols) | btree |

- `values`/`errors` are `NumpyArrayType(sa.Float)` → `float8[]`
  (`src/lightcurvedb/core/types.py`). `DataSet.align_to_observation()` fills gaps
  with `np.nan`, so arrays routinely contain `NaN`. At ~8 KB per array every row
  TOASTs.
- `db_scope()` (`src/lightcurvedb/io/pipeline/scope.py`) is the established
  session decorator; `chunkify()` (`src/lightcurvedb/util/iter.py`) the batching
  helper.
- Sentinel rows `id=0` in `photometric_source` / `processing_method` must exist
  before any `DataSet` insert.

**What does not exist**

- **No partition management code at all.** The only partition DDL in the repo is
  `tests/conftest.py:133-156`, which creates DEFAULT partitions.
- **No Alembic, no migrations, no `.sql` files.** Schema comes from
  `LCDBModel.metadata.create_all()`.
- **No versioning, supersession or validity flags.** `DataSet` does not even use
  `CreatedOnMixin` — its rows carry no timestamp. *Consequence: the new registry
  becomes the only record of when an observation's lightcurves were written.*
- No bulk-load/COPY helpers, despite `psycopg[binary]>=3.1.19` being a dependency.
- No test creates a real per-observation partition or exercises attach/detach;
  the whole suite runs against catch-all DEFAULT partitions.
- `tests/factories.py`, `tests/strategies/orm.py`, `tests/strategies/ingestion.py`
  are **dead code** referencing deleted models. Do not build on them.

`docs/source/schema.rst:533-537` says partitions "must be created by DBA before
inserting data". This work supersedes that.

### 3.1 Prior art in this repository — read before designing

The v1 codebase solved a closely related problem. Retrieve with
`git show <commit>:<path>`.

| Historical file | What it gives you |
|---|---|
| `lightcurvedb/core/psql_tables.py` | SQLAlchemy models over `pg_catalog`: `PGClass` (with `relispartition`, `relpartbound`, a `pg_get_expr(relpartbound, oid)` classmethod), `PGInherits`, `PGNamespace`, `PGIndex`; `PGClass.parent`/`.children` traverse `pg_inherits`. **Exactly the introspection layer needed.** |
| `lightcurvedb/models/table_track.py` | `PartitionTrack` / `RangedPartitionTrack` — a registry whose `oid` is an FK to `pg_class.oid`, reconciling provenance against catalog truth. |
| `lightcurvedb/util/merging.py` | Working `detach()`, `attach()`, rename and drop helpers, and a `merge_working_pair()` that is **already idempotent** — it detects an already-detached partition via `len(class_.parent) == 0` and continues. Reuse that resumability pattern. |
| `lightcurvedb/cli/partitioning.py` | `list_partitions`, `create_partitions`, `delete_partitions`; a **`--dryrun` mode** and confirmation before destructive DDL. Partitions lived in a dedicated `partitions` schema. |

Commits: `01d6c2c`, `aa5ed19`, `1b43711`, `ab4e4c8`, `a5b1185`, `74ea352`.

---

## 4. Confirmed defects and prerequisite fixes

Both verified by rendering the actual DDL via
`sa.schema.CreateTable(...).compile(postgresql.dialect())`.

### 4.1 `target_id` type mismatch — **fix first** (user decision)

```
dataset:           target_id            BIGINT  NOT NULL
datasethierarchy:  source_target_id     INTEGER NOT NULL   <-- wrong
                   child_target_id      INTEGER NOT NULL   <-- wrong
```

`Target.id` is `sa.BigInteger` (`src/lightcurvedb/models/target.py:181`) and
`dataset.target_id` inherits that through its scalar `sa.ForeignKey`. The
hierarchy columns are plain `orm.Mapped[int]` declared inside a composite
`sa.ForeignKeyConstraint` in `__table_args__`, and **SQLAlchemy does not
propagate types through composite FK constraints**.

PostgreSQL accepts an `int4 → int8` FK (there is an `int48eq` in the
`integer_ops` btree family), so nothing has failed yet. But `target.id` is a
surrogate `bigserial` and lineage becomes unrepresentable once it exceeds 2³¹.

- **FR-0a** Widen `source_target_id` and `child_target_id` to `BigInteger`.
- Until fixed, every validation anti-join must cast `h.source_target_id::bigint`.
- Rewriting hierarchy partitions is exactly what this project does anyway;
  deferring means rewriting them twice.

### 4.2 Intra-orbit lineage invariant — **declare and enforce** (user decision)

`datasethierarchy` is partitioned by `source_observation_id`, but
`fk_datasethierarchy_child` references `dataset` via `child_observation_id`. The
schema therefore permits a row with `source_observation_id=7,
child_observation_id=5` to live in hierarchy partition **7** while referencing
dataset partition **5** — which would make "swap observation 5" not a closed
unit. The user confirms lineage is always intra-orbit, so:

- **FR-0b** Add `CHECK (source_observation_id = child_observation_id)` to
  `datasethierarchy`, validate it once against existing data, and document it as
  the invariant that makes the `(dataset, datasethierarchy)` pair a closed
  atomic unit per observation.
- Validate with `ADD CONSTRAINT ... NOT VALID` then `VALIDATE CONSTRAINT`
  (`SHARE UPDATE EXCLUSIVE`, does not block reads or writes).
- **If validation fails**, cross-orbit rows exist, the premise is wrong, and the
  scope decision must be revisited before continuing. Report the count and stop.

### 4.3 Noted, not fixed here

`photometric_method_id`/`processing_method_id` have `default=0` in **Python
only** — the rendered DDL has no `DEFAULT 0`, so COPY must always name all six
columns explicitly. `values` is emitted unquoted and works (`VALUES` is a
`col_name_keyword`, not fully reserved) but handwritten SQL should use
`psycopg.sql.Identifier` regardless. Also outside scope: the dangling
`lcdb = 'lightcurvedb.cli:lcdbcli'` entry point; `template_dir = "templates"`
pointing at a nonexistent directory; and **`docker_runner` at the repo root is a
committed OpenSSH private key that should be rotated.**

---

## 5. Hard constraints

1. **LIST partition bounds cannot overlap.** Two partitions both claiming
   `FOR VALUES IN (5)` can never both be attached. During the coexistence window
   exactly one of {old, new} is attached; the other is a detached standalone
   table. *This is why "both versions visible to one ORM query" is unavailable
   without changing the primary key.*
2. **`datasethierarchy` holds composite FKs into `dataset`.** Behaviour on
   detach is spike-gated (§8, Q2).
3. **A DEFAULT partition makes `ATTACH` expensive and can make it fail.**
   PostgreSQL takes `ACCESS EXCLUSIVE` on the default and scans it to prove no
   row belongs to the incoming bound; if one does, the attach fails outright. A
   default partition may also forbid `DETACH CONCURRENTLY`.
4. **`DETACH PARTITION CONCURRENTLY` cannot run in a transaction block.** It
   therefore cannot participate in an atomic two-table swap. **Choose
   atomic-but-blocking.**
5. **`ATTACH` validates unless proven unnecessary.** A *valid* `CHECK`
   constraint matching the bound lets PostgreSQL skip the scan.
6. **Target deployment is PostgreSQL 14** (`docker-compose.yml`,
   `.github/workflows/test.yml`). `DETACH ... CONCURRENTLY` exists;
   `MERGE`/`SPLIT PARTITION` (PG 17) do not.

---

## 6. Scope

**In scope:** the two prerequisite fixes (§4.1, §4.2); `dataset` +
`datasethierarchy` swapped as one atomic unit per observation; a Python library
API; idempotent bootstrap helpers; provenance tracking; docs and tests.

**Out of scope:** `target_specific_time`; Alembic; a CLI; generating the
corrected lightcurve data (this provides the mechanism, not the science
pipeline); changing the `DataSet` primary key.

---

## 7. Functional requirements

### Prerequisites
- **FR-0a** Widen `datasethierarchy.{source,child}_target_id` to `BigInteger` (§4.1).
- **FR-0b** Add and validate the intra-orbit `CHECK` invariant (§4.2).

### Introspection
- **FR-1** List partitions of any partitioned table in `LCDBModel.metadata`:
  name, schema, oid, raw bound, parsed LIST value, default flag,
  `inhdetachpending`, row estimate, heap/index/TOAST/total size. Truth is
  `pg_catalog`, never a registry.
- **FR-2** Find the partition holding a given LIST value; report the default
  partition separately.
- **FR-3** Report a table's partition strategy and key columns; refuse to
  operate on an unpartitioned or non-LIST table.
- **FR-4** **Pre-flight attachability** before any lock is taken: column
  name/type/ordinal/`NOT NULL` parity against the parent, index-set parity
  (including that each index is `indisvalid` and PK-backed where the parent's
  is), presence of a *valid* bound-implying `CHECK`, FK parity, and whether a
  DEFAULT partition would block or slow the attach.

### Partition lifecycle
- **FR-5** Idempotently ensure a live partition exists for an observation. Use
  `CREATE TABLE ... PARTITION OF` for new empty orbits (auto-creates child
  indexes, no validation); the create-then-attach path is only for replacing
  populated ones.
- **FR-6** Create a detached standalone table structurally identical to a
  partition, carrying the bound-implying `CHECK` and (for `dataset`) mirrored
  outbound FKs.
- **FR-7** Bulk-load a staging table preserving `float8[]` semantics exactly —
  `NaN`, `±Inf`, empty arrays (`'{}'`, *not* NULL), NULL `errors`, 1-D only.
  psycopg3 binary `COPY` as the fast path, `executemany` as a fallback; both
  must produce byte-identical tables.
- **FR-8** Attach / detach / drop, with `CONCURRENTLY` available for retirement
  only and a clear error when attempted inside a transaction.
- **FR-9** **Atomic multi-table swap** in one transaction, with locks acquired
  up front in a canonical order, `lock_timeout` and `statement_timeout` set, and
  an in-transaction integrity gate.
- **FR-10** Roll back a promotion by re-attaching the retired relation.
- **FR-11** Drop retired relations, gated on explicit sign-off
  (`accepted_on IS NOT NULL`) with a dry-run default.

### Validation
- **FR-12** Expose a detached staging table as an ORM-queryable entity (§9.4 —
  sharp edges).
- **FR-13** Diff staged against live: key-set differences, cardinality
  mismatches, value deltas with `rtol`/`atol`/`equal_nan`. Must not transfer
  array payloads for rows that match.
- **FR-14** Detect staged hierarchy rows with no corresponding staged dataset
  row. PostgreSQL will not catch these (§8, Q2).
- **FR-15** Cheap partition summary for sign-off: row count, distinct targets,
  array-length histogram, NaN/Inf fractions, NULL `errors` count.

### Provenance and campaigns
- **FR-16** Record per physical relation: campaign, base table, observation,
  revision, physical name, reason, creator, expected/loaded row counts, and
  `created_on` / `loaded_on` / `indexed_on` / `validated_on` / `swapped_on` /
  `accepted_on` / `dropped_on`.
- **FR-17** Enforce **at most one live revision per slot** — structurally
  guaranteed by the LIST bound; assert it and fail loudly if violated.
- **FR-18** Validate state transitions against an explicit legal-transition map.
- **FR-19** Group revisions into a named campaign spanning several orbits;
  report progress.
- **FR-20** **Resumability**: a campaign interrupted by process death must
  resume from its label in a fresh process. All durable state lives in the
  registry and `pg_catalog`.
- **FR-21** **`reconcile()`** — re-derive state from `pg_catalog`, rewrite the
  registry to match, report every divergence corrected. **Catalog always wins.**
  Run at the start of every campaign invocation. Adopt hand-created
  `<base>_obs_<id>` partitions as revision 0.
- **FR-22** Assert the cross-table invariant that `dataset` and
  `datasethierarchy` are always at the **same live revision** for an
  observation. Divergence means a swap was not atomic; fail loudly.

### Bootstrap
- **FR-23** Idempotent helpers for registry tables, partitioned parents,
  per-observation partitions, and (development only) DEFAULT partitions. Safe to
  call on every process start. This is the substitute for a migration tool.

### Non-functional
- **NFR-1** Swap critical section O(1) in row count.
- **NFR-2** Every DDL entry point sets `lock_timeout` *and* `statement_timeout`
  by default. `lock_timeout` bounds how long you *wait*; `statement_timeout`
  bounds how long you *hold*. Without both, a swap queuing behind a long reader
  makes every subsequent reader queue behind the swap.
- **NFR-3** Every operation idempotent and resumable.
- **NFR-4** Primitives work against *any* LIST-partitioned table in the metadata.
- **NFR-5** No module may `import psycopg` at module scope —
  `docs/source/conf.py` mocks it and `sphinx-build -W` would fail. Import lazily.
- **NFR-6** Line length 79 (black/isort) / 81 (flake8); NumPy-style docstrings;
  `__repr__` and `__rich_repr__` on every new model.

---

## 8. Mandatory spikes — blocking

One `psql -f` against `docker compose up -d db`; under an hour. **Q1, Q2 and Q5
must be answered before implementation starts.** Record all findings in the PR.

| # | Question | Prior expectation | Gates |
|---|---|---|---|
| **Q1** | Is FK catalog growth O(N×M) in partition count? Create 10/20/40 partitions on both sides and count `pg_constraint` + internal `pg_trigger` rows; 4× per doubling means quadratic. | ~70% quadratic | **Whether to keep the composite FKs at all.** |
| **Q2** | Does detaching a *referenced* `dataset` partition silently leave dangling hierarchy rows? | ~70% yes, silent, no error | Whether the integrity gate is advisory or mandatory. |
| **Q3** | Does detaching a *referencing* `datasethierarchy` partition leave standalone **validated** FKs behind? | High yes | Whether the swap must strip FKs off the retired table. |
| **Q4** | Does `CHECK (observation_id = N)` let `ATTACH` skip the validation scan? | High yes | §9.6. Measure on an FK-free clone, or FK cloning pollutes the result. |
| **Q5** | Does `ATTACH` re-verify the referencing FK, and does a pre-created pre-validated FK get **adopted** (`conparentid` set) instead? | ~75% adopted | **Whether the swap window is ms or minutes.** |
| **Q6** | Cost of `ATTACH` with a DEFAULT partition present. | High: full scan | The no-default-partition recommendation. |
| **Q7** | Is `ADD FOREIGN KEY ... NOT VALID` allowed on a *partitioned* table in PG 14? | ~70% rejected (landed in PG 18) | Whether the NOT VALID strategy exists at all. |
| **Q8** | `DETACH CONCURRENTLY`: rejected in a transaction block? rejected with a default partition present? | High / ~80% | Confirms atomic-but-blocking is the only option. |
| **Q9** | What does plain `DETACH` leave on the detached table? | Medium | `drop_retired()` and rollback. |
| **Q10** | Does `LIKE INCLUDING ALL` from a *partitioned* parent copy the PK and indexes, and confirm it copies **no** FKs? | High | §9.5. The no-FK half is load-bearing. |
| **Q11** | Does unquoted `values` parse in `SELECT`/`COPY`? | High yes | Cosmetic; quote regardless. |
| **Q12** | Timed end-to-end swap dry-run at realistic scale, no default partition. | — | Go/no-go. |

**Q2 detail** — the minimal reproduction:

```sql
CREATE TABLE p (a int, b int, PRIMARY KEY (a, b)) PARTITION BY LIST (a);
CREATE TABLE p1 PARTITION OF p FOR VALUES IN (1);
CREATE TABLE c (a int, b int,
    CONSTRAINT fk FOREIGN KEY (a, b) REFERENCES p (a, b) ON DELETE CASCADE
) PARTITION BY LIST (a);
CREATE TABLE c1 PARTITION OF c FOR VALUES IN (1);
INSERT INTO p VALUES (1, 10);
INSERT INTO c VALUES (1, 10);

BEGIN;
  ALTER TABLE p DETACH PARTITION p1;          -- error, or silently allowed?
  SELECT count(*) FROM c;                      -- row still there?
  SELECT count(*) FROM c JOIN p USING (a, b);  -- join now empty?
ROLLBACK;
```

`DetachPartitionFinalize()` iterates `GetParentedForeignKeyRefs(partRel)` and,
for each sub-constraint where the partition is the *referenced* relation, clears
`conparentid` and `performDeletion()`s the constraint — i.e. it removes the FK
machinery and checks nothing. The strongest corroborating evidence is the
well-known sibling behaviour: `DROP TABLE` on a partition of an FK-referenced
partitioned table neither blocks nor cascades. **If confirmed, the integrity
burden is entirely on the application and the in-transaction gate in §9.7 is
mandatory, not optional.**

**Deliverable:** a version-pinned characterisation test
(`test_detaching_dataset_partition_with_live_hierarchy_rows`) asserting whatever
PG 14 actually does, so a PG 15/16/17 upgrade surfaces the change as a red test
rather than production data loss.

**If Q1 confirms the quadratic explosion**, seriously evaluate **dropping the two
composite FKs from the model**. Between catalog bloat (500 orbits → ~500k
constraint rows and their triggers), the §4.1 type mismatch, and the
silent-dangling behaviour of Q2, they buy little real integrity at high
operational cost. Replacing them with a scheduled anti-join audit plus
write-path enforcement makes every attach/detach pure catalog work. Present this
to the team as a real option.

---

## 9. Architecture

### 9.1 Module layout

```
src/lightcurvedb/core/partitions/
    __init__.py       curated public re-exports
    errors.py         PartitionError hierarchy
    naming.py         PartitionName: format/parse <base>_obs_<id>_v<rev> (pure)
    catalog.py        read-only pg_catalog queries -> frozen dataclasses
    state.py          OrbitState enum, derive_state(), assert_invariants()
    ddl.py            create_staging / build_indexes / add_fks / attach / detach / drop
    bulk.py           psycopg3 COPY loaders
    bootstrap.py      ensure_registry / ensure_partition / ensure_staging_table
    swap.py           OrbitReplacement: preflight / swap / rollback / drop_retired
    entity.py         physical table <-> ORM entity bridge
src/lightcurvedb/models/partition_revision.py    provenance model
```

**Layering rule (enforce in review):** `core/partitions/*` must not import
`lightcurvedb.models` or `lightcurvedb.io`; it operates on `sa.Table` / names and
a `sa.Connection`. That keeps it generic (NFR-4). `bootstrap.py` and `swap.py`
are the only modules aware of the domain pairing.

**Naming-collision discipline.** `lightcurvedb.util.iter.eq_partitions` is
in-memory list splitting. Nothing partition-DDL-related goes in `util/`; no
function is named `partitions()`/`eq_partitions()`/`split()`; every name carries
a qualifier (`list_table_partitions`, `find_partition_for_value`,
`ensure_partition`). The dataclass is `PartitionInfo`, never `Partition`. Add a
cross-referencing note to both docstrings.

### 9.2 Transaction contract — put this in every module docstring

> Primitives take a `sqlalchemy.Connection`; workflow methods take a
> `sqlalchemy.orm.Session`. **Neither commits.** The caller owns the transaction
> boundary, because that boundary *is* the atomicity guarantee for the swap.

`db_scope` wraps the body in `with session_factory()`, and `Session.__exit__`
**rolls back anything uncommitted** — so a `@db_scope()` function that emits DDL
and forgets `session.commit()` is a silent no-op. Conversely, decorating
`swap()` with `db_scope` would be actively wrong. `db_scope` therefore appears
only on a thin ribbon of one-shot entry points, each committing explicitly and
returning **frozen dataclasses, never ORM instances** (the session is closed on
return, and `expire_on_commit=False` leaves detached objects whose unloaded
relationships raise).

### 9.3 Naming: versioned, never renamed

```
<base>_obs_<observation_id>_v<revision>
```

Legacy `<base>_obs_<id>` = revision 0. Object names all explicit:
`..._pkey`, `..._target_idx`, `..._src_idx`, `..._child_idx`, `..._partcheck`.
Longest possible is `datasethierarchy_obs_2147483647_v999_child_idx` = 46 bytes,
safely under `NAMEDATALEN-1 = 63`. **This is a real constraint** —
auto-generated names from the 4- and 8-column index definitions would exceed 63
and silently truncate into collisions.

**No rename step.** The swap is detach-old / attach-new: four statements, zero
renames. "Which revision is live" is read from `pg_inherits`, not the name.
Renaming would add ~12 statements (tables *and* their indexes), a transient
name collision, and an extra failure mode in every recovery path. The
human-readable campaign label lives in the registry, not in the table name.

### 9.4 Verified SQLAlchemy findings — traps, not preferences

Confirmed against the installed SQLAlchemy 2.0.x. Encode each as a regression test.

1. **`orm.aliased(DataSet, <to_metadata copy>)` needs `adapt_on_names=True`.**
   With the default `False`, `Staged.target_id` compiles against the **live**
   table and `select(Staged)` raises `InvalidRequestError: Query contains no
   columns with which to SELECT from`. A `to_metadata` copy has fresh `Column`
   objects with no `corresponding_column` ancestry, so positional adaptation
   matches nothing. **Fails silently into wrong SQL.**
2. **`Table.to_metadata()` copies `postgresql_partition_by`.** Left in place,
   any `CreateTable` on the copy produces *another partitioned table*, which can
   never be attached. Strip it.
3. **`to_metadata` preserves explicitly-named constraints verbatim**
   (`pk_dataset`, `fk_datasethierarchy_source`, …). PostgreSQL index names are
   schema-global, so emitting `CreateTable` for a copy **collides on the PK's
   backing index**. → Generate staging DDL with raw
   `CREATE TABLE ... (LIKE parent INCLUDING ALL EXCLUDING INDEXES)` and use the
   SQLAlchemy `Table` purely as a client-side description.
4. **`DataSet.__table__.c.values` returns `ColumnCollection.values`, the bound
   method** — not the column. Always `.c["values"]`. (`Staged.values` on the
   AliasedClass is fine.)

**Staged-entity limitations, to be documented verbatim in the docstring and the
`.rst`:** (1) `adapt_on_names=True` mandatory; (2) **identity-map collision** —
a staged and a live row share a PK, so loading both as entities in one `Session`
yields *one* object; use column-level selects for comparison, or two Sessions;
(3) relationships don't know about the staging table — `Observation.datasets`
never sees staged rows and `source_datasets`/`derived_datasets` resolve through
`secondary="datasethierarchy"` to the **live** hierarchy, so pass
`.options(orm.raiseload("*"))`; (4) staged entities are read-only — aliasing
rewrites SELECT only, `session.add()` inserts into `dataset`; (5) `.c["values"]`;
(6) evict cached `Table` objects on drop.

*Rejected alternative worth a docstring paragraph:* `map_imperatively()` gives a
separate mapper with its own identity space and correct persistence, eliminating
(2) and (4) — rejected because it creates one never-collected mapper per
physical table (thousands over a campaign) and loses every hybrid property on
`DataSet`.

### 9.5 The staging table

```sql
CREATE TABLE IF NOT EXISTS public.dataset_obs_5_v4 (
    LIKE public.dataset INCLUDING ALL EXCLUDING INDEXES,
    CONSTRAINT dataset_obs_5_v4_partcheck CHECK (observation_id = 5)
);
```

`LIKE` options apply left-to-right, so `INCLUDING ALL EXCLUDING INDEXES` is
legal. Declaring the `CHECK` inline means COPY enforces it per row as data lands
(free) and the constraint is `convalidated` from birth.

**`LIKE ... INCLUDING ALL` carries over:** column names/types/collations/order;
**NOT NULL always** (regardless of `INCLUDING CONSTRAINTS`) — this is what makes
`observation_id` NOT NULL on the staging table and discharges half the partition
qual for free; CHECK constraints; defaults; **`attstorage` and `attcompression`**
— load-bearing here, since every row TOASTs; comments, extended statistics,
generated/identity columns.

**It does NOT carry over:** **foreign keys** (`LIKE` never copies FKs — verify in
Q10); the `PARTITION BY` clause (the copy is a plain heap, which is what we
want); triggers, rules, RLS, ACLs, tablespace.

Prefer `LIKE` over building from SQLAlchemy metadata: `to_metadata` needs three
separate corrections (§9.4 items 2–3) and silently drifts from anything a DBA
added out of band, whereas `LIKE` reads the live catalog.

**`CREATE TABLE IF NOT EXISTS` is a trap on its own.** If a prior run created the
table from an older parent definition, `IF NOT EXISTS` silently accepts the
stale shape and you discover the mismatch at attach time. `ensure_staging_table()`
must follow up with a symmetric `EXCEPT` over `pg_attribute` (attname, formatted
type, attnotnull, attnum) against the parent and fail loudly on any row.

### 9.6 Index build, and the PRIMARY KEY subtlety

Build after COPY, before attach:

```sql
SET LOCAL maintenance_work_mem = '4GB';
SET LOCAL max_parallel_maintenance_workers = 4;

ALTER TABLE public.dataset_obs_5_v4 ADD CONSTRAINT dataset_obs_5_v4_pkey
    PRIMARY KEY (observation_id, target_id, photometric_method_id, processing_method_id);
CREATE INDEX dataset_obs_5_v4_target_idx ON public.dataset_obs_5_v4 (target_id);
ANALYZE public.dataset_obs_5_v4;
```

**The PK must be a real `PRIMARY KEY` constraint, not a bare
`CREATE UNIQUE INDEX`.** `AttachPartitionEnsureIndexes()` walks the parent's
partitioned indexes and, for each backed by a constraint, requires the candidate
child index to *also* be constraint-backed (`get_relation_idx_constraint_oid()`
must return a valid OID) before adopting it. A matching-but-constraintless
unique index is rejected and PostgreSQL **rebuilds the index during ATTACH,
under `ACCESS EXCLUSIVE`, inside the swap window.** This is the easiest way to
turn a millisecond swap into a multi-minute outage.

**`ANALYZE` before the swap is not optional.** A freshly attached partition with
no `pg_statistic` rows gets default selectivity estimates; plans that used to
nested-loop can flip to sequential scans the instant you swap. Separately, PG 14
autovacuum does **not** analyse partitioned parents — schedule a periodic manual
`ANALYZE public.dataset` outside the swap window.

**The CHECK constraint stays after attach.** PostgreSQL does not drop it. Cost is
one `int4eq` per insert; benefits are a self-describing table after any future
detach, always-cheap re-attach, and a catalog-queryable assertion of which orbit
a physical table holds.

Write the CHECK plainly with a bare integer literal — `CHECK (observation_id = 5)`
yields `int4eq(Var, Const:int4)`, structurally identical to the qual
`get_qual_for_list()` generates. A `::bigint` cast yields `int48eq` and relies on
cross-type opfamily reasoning for no benefit. And it **must be VALID**:
`ConstraintImpliedByRelConstraint()` skips any `!ccvalid` entry, so a `NOT VALID`
CHECK is invisible to the prover and you get the full scan.

### 9.7 The swap

**Pre-flight, outside the transaction:** the FR-14 integrity queries; the index
parity assertion; and a check for long-running transactions holding
`AccessShareLock` on the parents, since our `ACCESS EXCLUSIVE` request would
queue behind them while every subsequent reader queues behind us.

```sql
BEGIN;
SET LOCAL lock_timeout      = '5s';    -- bound how long we WAIT
SET LOCAL statement_timeout = '120s';  -- bound how long we HOLD

-- All locks up front, one canonical order (every actor must use the same one),
-- so a contended run fails fast rather than halfway through.
LOCK TABLE public.dataset            IN ACCESS EXCLUSIVE MODE;
LOCK TABLE public.datasethierarchy   IN ACCESS EXCLUSIVE MODE;
LOCK TABLE public.observation        IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.photometric_source IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.processing_method  IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.target             IN SHARE ROW EXCLUSIVE MODE;

-- Guard: is the revision we think is live actually live?
-- (RAISE EXCEPTION if dataset_obs_5_v3 is absent or not relispartition)

ALTER TABLE public.datasethierarchy DETACH PARTITION public.datasethierarchy_obs_5_v3;
ALTER TABLE public.dataset          DETACH PARTITION public.dataset_obs_5_v3;
ALTER TABLE public.dataset          ATTACH PARTITION public.dataset_obs_5_v4          FOR VALUES IN (5);
ALTER TABLE public.datasethierarchy ATTACH PARTITION public.datasethierarchy_obs_5_v4 FOR VALUES IN (5);

-- Strip the now logically-violated FKs off the retired hierarchy table (Q3).
-- Detach converts inherited FKs into standalone VALIDATED constraints that now
-- reference rows no longer in the live dataset; pg_dump of that table would fail.
ALTER TABLE public.datasethierarchy_obs_5_v3 DROP CONSTRAINT fk_datasethierarchy_source;
ALTER TABLE public.datasethierarchy_obs_5_v3 DROP CONSTRAINT fk_datasethierarchy_child;

-- In-transaction integrity gate (mandatory if Q2 confirms silent dangling).
-- With the §4.2 invariant this is bounded to the swapped partition.

UPDATE public.partition_revision SET swapped_on = now()
 WHERE observation_id = 5 AND revision = 4 AND base_table IN ('dataset','datasethierarchy');
COMMIT;
```

**Ordering.** Referencing side (`datasethierarchy`) detaches **first** and
attaches **last**, so no visible instant has a hierarchy row referencing an
absent dataset row, and the referencing attach sees the new keys already in
place. Put the registry `UPDATE` **inside** the transaction — that removes an
entire row from the crash-recovery matrix.

**Making the referencing attach cheap.** `CloneFkReferencing()` looks for an
already-suitable constraint on the partition being attached (matched
structurally: same `confrelid`, key columns, update/delete/match actions, and
`convalidated`) and adopts it via `ConstraintSetParentConstraint()`, skipping
verification. This works cleanly for **`dataset`'s four outbound FKs** — they
reference non-partitioned live tables, so pre-create and pre-validate them
outside the lock window (names must match the parent's; read them from
`pg_constraint`, don't hardcode). It **cannot** work for `datasethierarchy`,
whose rows reference the not-yet-attached new dataset rows — chicken-and-egg.
Q5 measures what that costs; if Q7 shows `NOT VALID` is rejected on partitioned
tables, there is no alternative escape hatch.

**Rollback.** Before COMMIT: `ROLLBACK` — that is the entire undo, and it is the
whole reason for atomic-over-concurrent. After COMMIT and before
`drop_retired()`: the mirror swap, valid only while the retired relation exists
(exactly the window the coexistence requirement asks for). **Ordering hazard:**
re-adding the stripped FKs to the retired hierarchy table must happen *after*
the `dataset` re-attach, or they validate against an empty obs-5 key space and
fail. Alternatively add them `NOT VALID` and validate post-commit. Derive the
exact order from the Q12 dry-run. After `drop_retired()`: no undo — restore from
backup, which is why the drop is gated on `accepted_on IS NOT NULL`.

**Retry policy.** `lock_timeout` only bounds the wait. Catch SQLSTATE `55P03`
(`lock_not_available`), back off exponentially with jitter, and log
`pg_blocking_pids()` each failure. **Standby impact:** `ACCESS EXCLUSIVE`
replays on hot standbys and will cancel conflicting queries per
`max_standby_streaming_delay` — announce the window to replica readers.

### 9.8 Bulk load

Binary `COPY` via psycopg3, reaching the native connection through
`session.connection().connection.driver_connection` so the load joins the
session's transaction. `cur.copy(...)` with `cp.set_types([...])` — **mandatory
for `FORMAT BINARY`**, no inference. Use `sql.Identifier`, which quotes `values`
automatically. Convert with `ndarray.tolist()` (C-speed, native floats) rather
than passing ndarrays element-wise. Name all six columns explicitly (§4.3).
Validate `arr.ndim == 1`.

Binary is typically 2–4× faster than text for wide `float8[]` and avoids
17-digit float formatting; expose a `format="text"` flag for debugging.
`executemany` is 5–20× slower and stays for the registry and small fixtures.

**Decide explicitly whether "no measurement" is a `NaN` element or a `NULL`
element, and assert it in the loader.** `NaN` keeps array indices aligned with
`target_specific_time.barycentric_julian_dates`, which is almost certainly what
the science requires. Changing this convention mid-campaign would be silent
corruption.

**Load order:** create → `TRUNCATE` + `COPY` + `ANALYZE` (one transaction) →
indexes/PK → outbound FKs (dataset only) → pre-flight → swap.

**Sizing:** `pg_relation_size()` **excludes TOAST**, which will dominate here.
Always use `pg_total_relation_size()`. `INCLUDING STORAGE`/`INCLUDING COMPRESSION`
in the `LIKE` is load-bearing; if the build has LZ4, consider
`ALTER COLUMN values SET COMPRESSION lz4` on the parent so every staging table
inherits it. Don't bother with `UNLOGGED` + `SET LOGGED` — the rewrite costs more
than the saved WAL.

### 9.9 State, idempotency and concurrency

State is **derived from `pg_catalog`**, never stored:

```
ABSENT -> CREATED -> LOADED -> INDEXED -> FK_READY -> VERIFIED -> LIVE
                                                                   |
  (CREATED..VERIFIED) -> ABSENT            [abandon: DROP TABLE]   v
  previous LIVE -> RETIRED -> GONE         [drop_retired(), gated on accepted_on]
  RETIRED <-> LIVE                         [rollback, while both exist]
  DETACH_PENDING -> RETIRED                [ALTER TABLE ... DETACH ... FINALIZE]
```

Illegal states that must raise loudly: two revisions of a slot both attached
(impossible via the LIST bound, so it means the naming convention was violated);
and **`dataset` live at revision 4 while `datasethierarchy` is live at revision 3**
— the single most important cross-table invariant (FR-22), whose violation means
a swap was not atomic.

**Concurrency:** wrap per-orbit work in `pg_try_advisory_lock(<namespace>,
observation_id)` and skip-with-a-log on failure, so a 40-orbit campaign can be
run by several workers without coordination. Advisory locks are session-scoped,
so hold one connection per orbit's work.

**Load idempotency:** COPY appends and there is no upsert, so the loader does
`TRUNCATE` + `COPY` + `ANALYZE` **in one transaction**. `TRUNCATE` is
transactional, so a crash rolls back to the previous contents and the next run
simply truncates again. Do not attempt to resume mid-COPY — the bookkeeping is
more fragile than re-reading the source.

**Index builds are deliberately non-`CONCURRENTLY`** — the staging table is
invisible, so a full lock is free, and non-concurrent builds are transactional
and leave no `indisvalid=false` debris. Guard with `CREATE INDEX IF NOT EXISTS`
and, since there is no `ADD CONSTRAINT IF NOT EXISTS`, a `pg_constraint` existence
check in a `DO` block.

**Crash recovery** is uniform: every step re-derives state from the catalog and
resumes. The only genuinely ambiguous case — client dies after sending COMMIT,
before the ack — is resolved by reading `pg_inherits`, and `reconcile()` (FR-21)
backfills the registry.

---

## 10. Testing requirements

### 10.1 The fixture problem — must land first

`tests/conftest.py:133-156` creates DEFAULT partitions, which break or slow every
attach test. Decompose `v2_db` into composable fixtures whose **observable
contract is unchanged**, so none of the ~250 existing tests need edits:

- `database_engine` — connect (existing retry loop) + `create_all`.
- `_bound_session` — bind `LCDB_Session`, create sentinels, yield a Session.
- `default_partitions` — the three DEFAULT partitions.
- `v2_db(_bound_session, default_partitions)` — unchanged. *Argument order
  matters and needs a comment*: pytest resolves left to right.
- `partitioned_db(_bound_session)` — same schema and sentinels, **no defaults**.

**Teardown sweep is not optional.** `metadata.drop_all` only knows mapped tables.
Attached partitions die with their parent, but **detached staging and retired
tables survive**, and deterministic names mean the next test in the same worker
DB hits "relation already exists". Worse, a surviving staging table with a
mirrored FK to `observation` makes `DROP TABLE observation` fail, cascading into
confusing unrelated failures. Add `_drop_unmanaged_relations(engine)` before
`drop_all`: query `pg_class`/`pg_namespace` for `public` relations of kind
`r`/`p`, subtract `set(LCDBModel.metadata.tables)` **evaluated lazily at
teardown**, `DROP ... CASCADE` the rest. Lazy matters because
`tests/test_dataset_relationships.py:110` defines `class Orbit(LCDBModel)` at
module import, permanently adding `orbit` to shared metadata.

Promote `sample_mission` / `sample_catalog` / `sample_target` /
`sample_instrument` / `sample_observation` / `sample_photometric_source` /
`sample_processing_method` from `tests/test_dataset_relationships.py:18-110` to
`conftest.py`, rewritten against an `orm_session` indirection fixture that
partition modules override to `partitioned_db`. Module-local fixtures shadow
conftest ones, so promote and delete in two commits — the intermediate state is
green either way.

Add the currently-absent `[tool.pytest.ini_options]` with `markers` for `slow`
and `partitioning`, and wire up the installed-but-unused `pytest-timeout`. Leave
`-n auto` in `noxfile.py`.

### 10.2 Coverage

| Area | Kind | Notes |
|---|---|---|
| Naming round-trip, bound parsing, state reachability | **Hypothesis** | Pure, no DB. Highest yield per second. |
| `float8[]` COPY round-trip | **Hypothesis** | Domain **must** include `NaN`, `±Inf`, empty array, `errors=None`. Assert `np.array_equal(..., equal_nan=True)` and `dtype == float64`. |
| COPY vs `executemany` differential | **Hypothesis** | Same rows both ways into two tables; assert identical via SQL `EXCEPT` both directions. Strongest single test here. |
| Diff helper vs naive reference impl | **Hypothesis** | The two-pass SQL optimisation is exactly what is right on paper and wrong on NULLs. |
| Attach/detach round-trip, idempotency | Examples | Incl. `ensure_partition` second call is a no-op. |
| **FK behaviour with hierarchy rows present** | **Characterisation** | Output of Q2. Version-pinned with a comment. **Most valuable test here.** |
| FK adoption on attach | Example | Record the staging FK's `pg_constraint.oid` pre-attach; assert post-attach the same oid survives with `conparentid` set. Precise, not timing-based. |
| PK-constraint vs bare unique index on attach | Example | Guards §9.6 — the index-rebuild trap. |
| Attach blocked by conflicting default-partition row | Example | Parametrised `v2_db` vs `partitioned_db`. This test *is* the justification for `partitioned_db`. |
| Swap atomicity / rollback-on-failure | Examples | Sabotage the second step; assert the first is intact, both staging tables still exist, no registry row moved. |
| `lock_timeout` respected | Example, `slow` | Second connection holds `ACCESS SHARE`; expect SQLSTATE `55P03`, clean rollback. |
| Concurrent reader across swap | Examples, `slow` | A `REPEATABLE READ` reader must still see old rows after the swap commits — the detached table still physically exists. Surprising and load-bearing. |
| Revision skew between the two tables | Example | FR-22 must raise. |
| Rollback after drop | Example | Must raise. |
| Campaign resume in a fresh session | Example | The FR-20 contract. |
| Adopting a hand-created `dataset_obs_N` | Example | Uses the CHANGELOG DBA recipe; proves naming continuity (revision 0). |
| `adapt_on_names`, `partition_by` strip, identity-map collision | Examples | Regression guards for §9.4. The collision test is a *characterisation* test — it documents the footgun by asserting it. |
| Intra-orbit `CHECK` rejects a cross-orbit row | Example | Guards the §4.2 invariant. |
| `target_id` accepts a value > 2^31 | Example | Guards the §4.1 fix. |

**Never put `@given` on a test taking `partitioned_db`.** The fixture is
function-scoped, so Hypothesis raises `HealthCheck.function_scoped_fixture`, and
suppressing it means every example shares one database, one set of deterministic
table names, and one uncommitted transaction. Where a DB round-trip needs
generated data, draw a **list** at the top and exercise it in a single example.

New strategies in `tests/strategies/partitioning.py`, reusing
`tests/strategies/tess.py::orbits()` so keys look like real observation ids.

---

## 11. Documentation requirements

- **New `docs/source/partitioning.rst`**, in the `index.rst` toctree between
  `schema` and `models`. Sections: concepts; bootstrapping (and why no Alembic);
  staging; validating (the §9.4 limitations verbatim); promotion and rollback;
  retirement and drift; operational cautions; a Mermaid `stateDiagram-v2`;
  autodoc.
- **`docs/source/schema.rst`**: extend the "DataSet Partitioning" bullet (lines
  20-24); add the registry entities and the §4.2 invariant to the Mermaid ER
  diagram — *if it isn't in the diagram it doesn't exist as far as the team is
  concerned*; **rewrite constraint item 5 (lines 533-537)**, whose "partitions
  must be created by DBA" and "a default partition handles unexpected
  observation IDs" are now both wrong and actively harmful.
- **`CHANGELOG.md`**: feature bullets under `[Unreleased]`; reframe "Database
  Administration Notes" (lines 82-90) as the legacy manual workflow, keeping the
  SQL as documentation of what the library emits, led by the Python equivalent;
  extend the Migration Guide with `ensure_registry_tables()` →
  `reconcile()` → the two prerequisite fixes → drop production DEFAULT partitions.

**`sphinx-build -W` hygiene — three traps**, since `docs.yml` treats warnings as
errors: (1) `api.rst` marks every entry `:no-index:`, so each object must be
canonically indexed exactly once elsewhere — **models canonical in `models.rst`**,
**partitioning callables canonical in `partitioning.rst`**, `api.rst` keeps
`:no-index:` copies, and `partitioning.rst` uses `:no-index:` when it re-shows
`PartitionRevision`; (2) `conf.py` mocks `psycopg` (NFR-5); (3) every new `.rst`
needs a title underline at least as long as the title and exactly one toctree entry.

---

## 12. Delivery sequencing

Branch `feature/<topic>` → PR into `staging` → beta → PR to `master`.
Conventional commits enforced twice (pre-commit `commit-msg` hook +
`commitlint.yml`).

| # | Branch | Subject | Depends on |
|---|---|---|---|
| 0 | `spike/partition-fk-detach` | *(throwaway)* run Q1–Q12, record findings | — |
| 0b | `fix/datasethierarchy-target-id-width` | `fix(models): widen datasethierarchy target ids to BigInteger` | — |
| 0c | `feat/intra-orbit-lineage-invariant` | `feat(models): enforce intra-orbit lineage on datasethierarchy` | 0b |
| 1 | `feature/partition-test-fixtures` | `test: decompose v2_db into composable database fixtures` | — |
| 2 | `feature/partition-naming` | `feat(core): add partition naming policy and error types` | 1 |
| 3 | `feature/partition-catalog` | `feat(core): add PostgreSQL partition introspection` | 2 |
| 4 | `feature/partition-ddl` | `feat(core): add partition DDL primitives` | 0, 3 |
| 5 | `feature/partition-registry` | `feat(models): add partition revision provenance` | 1 |
| 6 | `feature/staged-entity` | `feat(core): expose detached partitions as ORM entities` | 2 |
| 7 | `feature/partition-bulk-load` | `feat(core): add binary COPY loaders for partition staging` | 4, 6 |
| 8 | `feature/partition-swap` | `feat(core): add atomic multi-table partition swap` | 0, 4 |
| 9 | `feature/partition-reconcile` | `feat(core): reconcile partition registry against pg_catalog` | 3, 5 |
| 10 | `feature/dataset-comparison` | `feat(core): add staged-vs-live dataset comparison` | 6, 7 |
| 11 | `feature/replacement-campaign` | `feat(core): add orbit replacement campaign workflow` | 5, 7, 8, 9, 10 |
| 12 | `feature/partitioning-docs` | `docs: document partition management and lightcurve replacement` | 11 |

**Rationale.** 0 first because Q1 can invalidate the composite FKs and Q2/Q5
change the swap. 0b/0c are the prerequisite fixes and must precede any hierarchy
partition being written, or they get rewritten twice. 1 lands alone so a fixture
regression is unambiguous, with ~250 existing tests as its regression suite.
2→3→4 strict bottom-up. 5 and 6 are independent, parallelisable. 8 separate from
4 so the riskiest DDL gets a dedicated review. 11 is the keystone but small,
because everything it composes is already tested. 12 last — but draft the §9.4
limitations during PR 6 while findings are fresh.

**Sequencing hazard:** PR 5 adds tables to `LCDBModel.metadata`, so every test's
`create_all`/`drop_all` grows. Measure suite runtime before and after; if it
regresses, the fix is session-scoped `create_all` with per-test truncation — a
worthwhile follow-up, not to be smuggled into this feature.

---

## 13. Verification

```bash
docker-compose up -d db          # postgres:14, matches CI
pip install -e ".[dev]"
```

1. **Spikes** — run Q1–Q12 against the scratch DB; record outputs in the PR.
2. **Prerequisites** — after 0b/0c, confirm `VALIDATE CONSTRAINT` succeeds on
   production-shaped data. If the intra-orbit check fails, **stop** and revisit
   scope.
3. **Unit/integration** — `pytest -n auto` (or `nox`, 3.11 + 3.12). All ~250
   existing tests must stay green after PR 1 **with no test-file edits** — that
   is the success criterion for the fixture refactor.
4. **Full campaign rehearsal** as
   `tests/test_replacement_campaign.py::test_end_to_end_replacement`:
   bootstrap registry + partitions for 3 observations; load known-bad data;
   open a campaign, stage all 3, bulk-load corrected data;
   **assert ordinary `session.query(DataSet)` still returns the old data** (the
   core coexistence requirement); assert the staged entity returns the new data;
   run the diff and assert it reports exactly the injected differences;
   promote one observation and assert the other two are untouched; roll it back
   and assert the old data is live again; promote all three; `drop_retired()`;
   assert `reconcile()` is clean and FR-22 holds.
5. **Resumability** — discard the session between stage and promote,
   `resume(label)` in a fresh session, complete, assert the same end state.
6. **Lock behaviour** — with a `REPEATABLE READ` reader open on `dataset`, run a
   promotion with `lock_timeout="100ms"`; assert clean failure on SQLSTATE
   `55P03` with nothing changed.
7. **Docs** — `nox -s docs` (runs `sphinx-build -W`).
8. **Lint/type** — `pre-commit run --all-files`; `mypy src`.

---

## 14. Open questions for the team

1. **Who signs off, and what is the acceptance artifact?** The diff report is
   designed to be it. If a persisted reviewable record is wanted,
   `partition_revision.notes` (JSONB) should carry the diff summary and the
   approver — cheap now, awkward later.
2. **How long are retired partitions kept after promotion?** Sets the default
   for `drop_retired(older_than=...)` and the disk headroom: during a campaign
   an orbit's storage roughly doubles.
3. **Do production databases currently have DEFAULT partitions on `dataset`?**
   If so, dropping them is a prerequisite (constraint 3) and belongs in the
   migration guide as a discrete scheduled step. Check also whether any rows
   have accumulated in them.
4. **NaN vs NULL for "no measurement"** (§9.8) — needs an explicit ruling before
   the first load, since changing it mid-campaign is silent corruption.
5. **Is `target_specific_time` genuinely unaffected?** Scoped out on the premise
   that the bug is in flux values only. If a rebuild also changes barycentric
   times, the swap must extend to a third table — the design supports it, but
   tests and docs would need extending.
