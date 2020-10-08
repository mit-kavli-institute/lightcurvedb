"""
This module defines constants across all submodules
"""
import os

DEFAULT_ROOT = os.path.join('/', 'pdo')
QLP_PATH = os.path.join(DEFAULT_ROOT, 'qlp-data')
QLP_ORBITS = QLP_PATH

POC_PATH = os.path.join(DEFAULT_ROOT, 'poc-data')
POC_ORBITS = os.path.join(POC_PATH, 'orbits')
