"""
This module defines constants across all submodules.

Note: Path constants below are TESS-specific and retained for
backward compatibility. New mission-specific constants should
be defined in mission-specific modules.
"""
import os

DEFAULT_SCRATCH = os.path.join("/", "scratch", "tmp")
DEFAULT_ROOT = os.path.join("/", "pdo")

# TESS-specific paths (retained for backward compatibility)
QLP_PATH = os.path.join(DEFAULT_ROOT, "qlp-data")
QLP_ORBITS = QLP_PATH  # TESS Quick Look Pipeline orbit data
POC_PATH = os.path.join(DEFAULT_ROOT, "poc-data")
POC_ORBITS = os.path.join(POC_PATH, "orbits")  # TESS POC orbit data
QLP_SECTORS = os.path.join(QLP_PATH, "sector-{sector}")  # TESS sector data

# General paths
CACHE_DIR = os.path.join(DEFAULT_SCRATCH, "lcdb_ingestion")
