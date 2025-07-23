#!/usr/bin/env python3
"""
Generate database schema diagram using Paracelsus.
This script can be run directly without installing all project dependencies.
"""

import subprocess
import sys
from pathlib import Path


def main():
    # Check if paracelsus is installed
    try:
        import paracelsus  # noqa: F401
    except ImportError:
        print("Installing paracelsus...")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "paracelsus"]
        )

    # Ensure output directory exists
    output_dir = Path("docs/source/_static")
    output_dir.mkdir(exist_ok=True)

    # Generate the diagram
    cmd = [
        sys.executable,
        "-m",
        "paracelsus",
        "graph",
        "lightcurvedb.core.base_model:LCDBModel",
        "--import-module",
        "lightcurvedb.models.frame",
        "--import-module",
        "lightcurvedb.models.instrument",
        "--import-module",
        "lightcurvedb.models.observation",
        "--import-module",
        "lightcurvedb.models.dataset",
        "--import-module",
        "lightcurvedb.models.target",
        "--import-module",
        "lightcurvedb.models.quality_flag",
        "-o",
        "docs/source/_static/schema_auto_generated.mmd",
    ]

    print("Generating schema diagram...")
    try:
        subprocess.check_call(cmd)
        output_file = output_dir / "schema_auto_generated.mmd"
        print(f"Successfully generated schema diagram at {output_file}")
    except subprocess.CalledProcessError as e:
        print(f"Error generating schema: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
