"""
This module provisions static files needed for testing. This functionality
requires a network connection in order to communicate with JPL's Horizon
interface.

This module will sweep month-wise.
"""

import pathlib
import re

import pandas as pd
import requests
from loguru import logger

from lightcurvedb import models

from .constants import TEST_PATH

ROOT_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"
MISSION_START = "2018-05-01"
OBSERVATION_END = "2023-12-01"


def get_tess_positions(start_date, end_date, step_size=None):
    logger.debug(
        f"Pulling from JPL HORIZON for dates {start_date} to {end_date}"
    )
    payload = {
        "CENTER": "500@0",
        "COMMAND": "-95",
        "CSV_FORMAT": "YES",
        "EPHEM_TYPE": "VECTORS",
        "MAKE_EPHEM": "YES",
        "OBJ_DATA": "NO",
        "OUT_UNITS": "AU-D",
        "REF_PLANE": "FRAME",
        "START_TIME": start_date.isoformat().split(" ")[0],
        "STEP_SIZE": "1H" if step_size is None else step_size,
        "STOP_TIME": end_date.isoformat().split(" ")[0],
    }
    response = requests.get(ROOT_URL, params=payload)
    data = response.json()["result"]

    # Process JPL response
    search = re.search(r"\$\$SOE([^\$]+)\$\$EOE", data)
    if search is None:
        raise RuntimeError(
            "JPL HORIZON response invalid. "
            f"Request URL: {response.url}. "
            f"Return was: {response.json()}"
        )
    search = search.groups()[0]
    lines = [
        line.strip().split(",") for line in search.split("\n") if len(line) > 1
    ]
    parsed = [
        (float(t[0]), t[1].strip(), *tuple(float(_) for _ in t[2:]))
        for *t, _tail in lines
    ]
    df = pd.DataFrame(
        parsed,
        columns=[
            "JDTDB",
            "TDB",
            "X",
            "Y",
            "Z",
            "VX",
            "VY",
            "VZ",
            "LT",
            "RG",
            "RR",
        ],
    )
    return df


def read_or_retrieve_positions(start_date):
    truncated_date = start_date.replace(day=1, second=0, microsecond=0)
    expected_name = f"{start_date.isoformat()}.eph"
    expected_path = pathlib.Path(TEST_PATH) / expected_name
    try:
        df = pd.read_csv(expected_path)
    except FileNotFoundError:
        next_month = truncated_date.month + 1
        year = truncated_date.year
        if next_month > 12:
            next_month = 1
            year += 1

        end_month = truncated_date.replace(year=year, month=next_month)
        df = get_tess_positions(truncated_date, end_month)
        df.to_csv(expected_path, index=False)

    return df


def load_ephemeris(db, ephemeris):
    mask = set()
    objects = []
    for _, row in ephemeris.iterrows():
        if row["JDTDB"] in mask:
            continue
        obj = models.SpacecraftEphemeris(
            barycentric_dynamical_time=row["JDTDB"],
            x_coordinate=row["X"],
            y_coordinate=row["Y"],
            z_coordinate=row["Z"],
            light_travel_time=row["LT"],
            range_to=row["RG"],
            range_rate=row["RR"],
        )
        mask.add(row["JDTDB"])
        objects.append(obj)

    db.add_all(objects)
    db.commit()


def sync_tess_positions(db):
    date_range = pd.date_range(MISSION_START, OBSERVATION_END, freq="MS")
    positions = []
    for month_ts in date_range.tolist():
        positions.append(read_or_retrieve_positions(month_ts))
    full_range = pd.concat(positions)
    load_ephemeris(db, full_range)
