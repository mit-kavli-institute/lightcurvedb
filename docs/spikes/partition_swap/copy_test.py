import os
import time

import numpy as np
import psycopg
from psycopg import sql

DSN = os.environ.get(
    "LCDB_SPIKE_DSN",
    "postgresql://{u}:{p}@{h}:{P}/postgres".format(
        u=os.environ.get("POSTGRES_USER", "postgres"),
        p=os.environ.get("POSTGRES_PASSWORD", "postgres"),
        h=os.environ.get("POSTGRES_HOST_LOCAL", "localhost"),
        P=os.environ.get("POSTGRES_PORT", "5432"),
    ),
)
COLS = (
    "observation_id",
    "target_id",
    "photometric_method_id",
    "processing_method_id",
    "values",
    "errors",
)
TYPES = ("int4", "int8", "int4", "int4", "float8[]", "float8[]")

rows = [
    (70, 1, 0, 0, np.array([np.nan, np.inf, -np.inf, 1.5]), None),
    (
        70,
        2,
        0,
        0,
        np.array([], dtype=np.float64),
        np.array([], dtype=np.float64),
    ),
    (70, 3, 0, 0, np.array([1.0, 2.0]), np.array([np.nan])),
    (70, 4, 0, 0, np.array([1e308, -1e308, 5e-324]), None),
]

with psycopg.connect(DSN, autocommit=True) as conn:
    conn.execute("SET search_path = spike")
    conn.execute("DROP TABLE IF EXISTS copy_probe")
    conn.execute(
        """CREATE TABLE copy_probe (
        observation_id int NOT NULL, target_id bigint NOT NULL,
        photometric_method_id int NOT NULL, processing_method_id int NOT NULL,
        values float8[] NOT NULL, errors float8[])"""
    )

    stmt = sql.SQL("COPY {} ({}) FROM STDIN (FORMAT BINARY)").format(
        sql.Identifier("copy_probe"),
        sql.SQL(", ").join(sql.Identifier(c) for c in COLS),
    )
    with conn.cursor() as cur, cur.copy(stmt) as cp:
        cp.set_types(TYPES)
        for o, t, p, pr, v, e in rows:
            cp.write_row(
                (o, t, p, pr, v.tolist(), None if e is None else e.tolist())
            )

    print("=== binary COPY round-trip ===")
    ok = True
    with conn.cursor() as cur:
        cur.execute(
            "SELECT target_id, values, errors"
            " FROM copy_probe ORDER BY target_id"
        )
        for (tid, v, e), orig in zip(cur.fetchall(), rows):
            gv = np.array(v, dtype=np.float64)
            exp_v = orig[4]
            match_v = gv.shape == exp_v.shape and np.array_equal(
                gv, exp_v, equal_nan=True
            )
            exp_e = orig[5]
            if exp_e is None:
                match_e = e is None
            else:
                ge = np.array(e, dtype=np.float64)
                match_e = ge.shape == exp_e.shape and np.array_equal(
                    ge, exp_e, equal_nan=True
                )
            ok &= match_v and match_e
            vs = str(v).ljust(34)
            es = str(e).ljust(8)
            print(
                "  tid={} values={} errors={} "
                "values_ok={} errors_ok={} dtype={}".format(
                    tid, vs, es, match_v, match_e, gv.dtype
                )
            )
    print("ALL MATCH:", ok)

    # differential: COPY vs executemany
    conn.execute("DROP TABLE IF EXISTS exec_probe")
    conn.execute("CREATE TABLE exec_probe (LIKE copy_probe INCLUDING ALL)")
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO exec_probe VALUES (%s,%s,%s,%s,%s,%s)",
            [
                (o, t, p, pr, v.tolist(), None if e is None else e.tolist())
                for o, t, p, pr, v, e in rows
            ],
        )
    diff = (
        "SELECT count(*) FROM"
        " (SELECT * FROM {a} EXCEPT SELECT * FROM {b}) x"
    )
    d1 = conn.execute(diff.format(a="copy_probe", b="exec_probe")).fetchone()[
        0
    ]
    d2 = conn.execute(diff.format(a="exec_probe", b="copy_probe")).fetchone()[
        0
    ]
    print(
        "=== COPY vs executemany differential:"
        " {} / {} (both 0 == identical) ===".format(d1, d2)
    )

    # throughput
    big = [
        (71, i, 0, 0, np.random.random(1000).tolist(), None)
        for i in range(20000)
    ]
    conn.execute("TRUNCATE copy_probe")
    t0 = time.time()
    with conn.cursor() as cur, cur.copy(stmt) as cp:
        cp.set_types(TYPES)
        for r in big:
            cp.write_row(r)
    tc = time.time() - t0
    conn.execute("TRUNCATE exec_probe")
    t0 = time.time()
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO exec_probe VALUES (%s,%s,%s,%s,%s,%s)", big
        )
    te = time.time() - t0
    print(
        "=== 20k rows x 1000-elem float8[]:"
        " COPY {}s | executemany {}s | {}x ===".format(
            round(tc, 2), round(te, 2), round(te / tc, 1)
        )
    )
