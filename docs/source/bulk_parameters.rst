===========================
Writing Parameters at Scale
===========================

This guide shows how to load astrophysical parameters for very large target
sets -- on the order of tens of millions of targets -- efficiently and
idempotently. The running example is **25 million targets**, each carrying a
handful of measured quantities (say ~20), i.e. roughly **500 million**
:class:`~lightcurvedb.models.AstroParameter` rows. The technique scales
linearly: halve the parameters-per-target and you halve the rows.

The data model in one minute
============================

Two models matter here (see :doc:`schema` for the full picture):

* :class:`~lightcurvedb.models.ParameterKind` -- a **named quantity-kind**. Its
  ``name`` is unique (the keyword, e.g. ``"effective_temperature"``) and
  ``unit_str`` holds the astropy unit (e.g. ``"K"``). This is a *tiny* lookup
  table: one row per distinct quantity, a few dozen rows in practice.
* :class:`~lightcurvedb.models.AstroParameter` -- one measured ``value`` (with
  asymmetric ``upper_error`` / ``lower_error``) for a target, referencing a
  :class:`~lightcurvedb.models.Target` (``target_id``) and a
  :class:`~lightcurvedb.models.ParameterKind` (``kind_id``). This is the *large*
  table. A ``UniqueConstraint`` on ``(target_id, kind_id)`` means a target holds
  at most one parameter per kind.

Because the parameter *name* lives once per kind in ``parameter_kind`` and each
``astro_parameter`` row only carries a 4-byte ``kind_id``, the name costs
essentially nothing per row at scale -- which is exactly why bulk loading is
cheap.

.. note::

   ``AstroParameter.name`` is **read-only** -- it mirrors ``kind.name`` through
   an association proxy. You never write it; you set the name on the
   :class:`~lightcurvedb.models.ParameterKind`. Likewise, ``value`` is stored
   *verbatim* in the kind's unit: the database performs no unit conversion, so
   express the value in the kind's unit before writing it.

Why SQLAlchemy Core
===================

The ORM's unit-of-work (identity map, autoflush, per-object bookkeeping) is
convenient for a handful of objects but ruinous for hundreds of millions. For
bulk loads, drive inserts with SQLAlchemy **Core** and an *executemany* list of
plain dicts -- no model instances are created, and the work goes to the driver
in one round trip per batch.

The examples assume you have a ``session`` (a SQLAlchemy
:class:`~sqlalchemy.orm.Session`). See :doc:`connecting` for the
``db_scope`` decorator and other ways to obtain one.

Step 1 -- Establish the parameter-kind vocabulary
=================================================

There is no built-in get-or-create for kinds, but the set is small and fixed,
so upsert it once and read back a ``{name: id}`` map. ``on_conflict_do_nothing``
on the unique ``name`` makes this idempotent across re-runs.

.. code-block:: python
    :linenos:

    import astropy.units as u
    from sqlalchemy import select
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    from lightcurvedb.models import ParameterKind

    # (name, astropy unit) for every quantity you will write.
    KIND_SPECS = [
        ("effective_temperature", u.K),
        ("radius", u.solRad),
        ("mass", u.solMass),
        ("distance", u.pc),
        # ...
    ]

    def ensure_kinds(session, kind_specs):
        """Idempotently create the kinds and return a {name: id} map."""
        session.execute(
            pg_insert(ParameterKind).on_conflict_do_nothing(
                index_elements=["name"]
            ),
            [{"name": name, "unit_str": str(unit)} for name, unit in kind_specs],
        )
        session.commit()
        return dict(
            session.execute(
                select(ParameterKind.name, ParameterKind.id)
            ).all()
        )

``str(u.K)`` yields ``"K"`` -- the same string
:meth:`ParameterKind.reflect_astropy_unit
<lightcurvedb.models.ParameterKind.reflect_astropy_unit>` would store -- so the
unit round-trips through :meth:`ParameterKind.as_unit
<lightcurvedb.models.ParameterKind.as_unit>` on read.

Step 2 -- Resolve target ids
============================

``AstroParameter.target_id`` is a foreign key, so the
:class:`~lightcurvedb.models.Target` rows must already exist. If your source
data identifies targets by catalog name, build a lookup once:

.. code-block:: python
    :linenos:

    from lightcurvedb.models import Target

    def target_id_map(session, catalog_id):
        """Map a catalog's target names to their database ids."""
        rows = session.execute(
            select(Target.name, Target.id).where(
                Target.catalog_id == catalog_id
            )
        ).all()
        return {name: id_ for name, id_ in rows}

At 25M targets this map is a few hundred MB in memory; if that is too much,
resolve ids in chunks aligned to your input batches instead of all at once.

Step 3 -- Bulk-insert the parameters
====================================

Feed Core a list of plain dicts. **Omit** ``id`` (the database assigns it from a
sequence) and **omit** ``name`` (it is the read-only proxy). Each dict carries
only the real columns:

.. code-block:: python
    :linenos:

    from sqlalchemy import insert
    from lightcurvedb.models import AstroParameter

    batch = [
        {
            "target_id": 1001,
            "kind_id": kind_ids["effective_temperature"],
            "value": 5772.0,
            "upper_error": 50.0,
            "lower_error": 40.0,
        },
        # ... up to ~batch_size rows ...
    ]
    session.execute(insert(AstroParameter), batch)
    session.commit()

Generate ``batch`` lazily from your input stream -- never materialise all 500M
dicts at once.

Idempotent re-runs (upsert)
===========================

A plain ``insert`` raises on a duplicate ``(target_id, kind_id)``. To make loads
safely re-runnable -- updating values in place rather than failing -- use the
PostgreSQL ``ON CONFLICT`` form keyed on the unique constraint:

.. code-block:: python
    :linenos:

    from sqlalchemy.dialects.postgresql import insert as pg_insert

    stmt = pg_insert(AstroParameter)
    stmt = stmt.on_conflict_do_update(
        index_elements=["target_id", "kind_id"],
        set_={
            "value": stmt.excluded.value,
            "upper_error": stmt.excluded.upper_error,
            "lower_error": stmt.excluded.lower_error,
        },
    )
    session.execute(stmt, batch)
    session.commit()

Use :meth:`~sqlalchemy.dialects.postgresql.Insert.on_conflict_do_nothing`
instead if existing values should be left untouched. Both forms work with the
*executemany* batch list shown above.

Batching and commit cadence
===========================

* **Batch size.** ~10k-50k rows per ``execute`` is a good range. Larger batches
  amortise round trips but use more memory and lengthen each statement.
* **Commit cadence.** Commit every few batches (e.g. every 10) rather than once
  at the very end. This bounds transaction size, WAL growth, and lock
  duration, and lets a failed run resume near where it stopped (the upsert makes
  re-processing a committed batch a no-op).
* **One session, streamed input.** Reuse a single session for the whole job and
  generate batches from an iterator so memory stays flat. The global
  ``LCDB_Session`` is configured with ``expire_on_commit=False``, so committing
  mid-loop will not trigger reloads.

Reading parameters back
=======================

Once loaded, a target's parameters are reachable by keyword:

.. code-block:: python
    :linenos:

    target = session.get(Target, 1001)

    # dict view keyed by parameter name
    teff = target.parameters_by_name["effective_temperature"]
    teff.value            # 5772.0

    # __getitem__ returns an astropy Quantity (value * unit)
    target["effective_temperature"]      # <Quantity 5772. K>
    teff.as_quantity()                   # equivalent

.. note::

   ``parameters_by_name`` is a view loaded from the database, so refresh or
   re-fetch the target after a bulk write (``session.refresh(target)``) before
   reading it in the same session.

Pitfalls
========

.. warning::

   Never assign ``AstroParameter.name``. It is an association proxy onto
   ``kind.name``; writing it would rename the *shared*
   :class:`~lightcurvedb.models.ParameterKind` for every target that uses it.
   Set names on the kind, in Step 1.

* **Duplicate keys within a batch.** Two rows with the same
  ``(target_id, kind_id)`` in one ``execute`` abort the whole batch unless you
  use the ``ON CONFLICT`` form. De-duplicate your input.
* **Kinds cannot be deleted while in use.** ``kind_id`` is ``ON DELETE
  RESTRICT``; curate the vocabulary up front rather than deleting kinds later.
* **Targets first.** ``target_id`` is a foreign key -- load
  :class:`~lightcurvedb.models.Target` rows before their parameters.
* **Not partitioned.** ``astro_parameter`` is a plain table. The design handles
  hundreds of millions of rows comfortably; at the multi-billion-row scale you
  may later want PostgreSQL partitioning (out of scope here).

Putting it together
===================

A complete, re-runnable loader. Input ``rows`` is any iterable of
``(target_id, kind_name, value, upper_error, lower_error)`` tuples:

.. code-block:: python
    :linenos:

    from itertools import islice

    import astropy.units as u
    from sqlalchemy import select
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    from lightcurvedb.io.pipeline import db_scope
    from lightcurvedb.models import AstroParameter, ParameterKind

    KIND_SPECS = [
        ("effective_temperature", u.K),
        ("radius", u.solRad),
        ("mass", u.solMass),
    ]

    def _batched(iterable, n):
        it = iter(iterable)
        while chunk := list(islice(it, n)):
            yield chunk

    @db_scope()
    def bulk_write_parameters(session, rows, batch_size=20_000):
        # Step 1: kinds + {name: id}
        session.execute(
            pg_insert(ParameterKind).on_conflict_do_nothing(
                index_elements=["name"]
            ),
            [{"name": n, "unit_str": str(u_)} for n, u_ in KIND_SPECS],
        )
        kind_id = dict(
            session.execute(
                select(ParameterKind.name, ParameterKind.id)
            ).all()
        )

        # Steps 3 + upsert: stream batches
        stmt = pg_insert(AstroParameter)
        stmt = stmt.on_conflict_do_update(
            index_elements=["target_id", "kind_id"],
            set_={
                "value": stmt.excluded.value,
                "upper_error": stmt.excluded.upper_error,
                "lower_error": stmt.excluded.lower_error,
            },
        )

        for i, chunk in enumerate(_batched(rows, batch_size), start=1):
            session.execute(
                stmt,
                [
                    {
                        "target_id": target_id,
                        "kind_id": kind_id[kind_name],
                        "value": value,
                        "upper_error": upper,
                        "lower_error": lower,
                    }
                    for target_id, kind_name, value, upper, lower in chunk
                ],
            )
            if i % 10 == 0:
                session.commit()
        session.commit()
