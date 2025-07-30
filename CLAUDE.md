# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the lightcurve-database project (lcdb), a PostgreSQL-backed system for storing and retrieving astronomical time-series data from the TESS (Transiting Exoplanet Survey Satellite) mission. The codebase is currently on the `version-2` branch undergoing significant refactoring.

## Key Commands

### Development Setup
```bash
# Install the package in development mode
pip install -e ".[dev]"

# Run tests across multiple Python versions
nox

# Run tests with pytest directly
pytest

# Build documentation
nox -s docs

# Format code
black src tests

# Type checking
mypy src
```

### Docker Development
```bash
# Start PostgreSQL database
docker-compose up -d

# Run tests in Docker environment
docker-compose run test
```

## Architecture Overview

### Core Components

1. **SQLAlchemy Models** (`src/lightcurvedb/models/`)
   - Database entities: Frame, Instrument, Observation, Orbit, Target, Lightcurve
   - Uses SQLAlchemy 2.0+ with PostgreSQL backend
   - Models define relationships between astronomical observations and their metadata

2. **Database Connection** (`src/lightcurvedb/core/`)
   - Configuration via `~/.config/lightcurvedb/db.conf`
   - Connection pooling and session management
   - Base model definitions and constants

3. **Scientific Utilities** (`src/lightcurvedb/util/`)
   - TESS-specific functionality for handling telescope data
   - Plotting utilities for lightcurve visualization
   - SQL operation helpers
   - Logging configuration with loguru

### Data Structure

Lightcurves are stored as inline arrays per orbit, identified by TIC (TESS Input Catalog) IDs. Each lightcurve contains:
- Time series data (magnitudes/flux values)
- Barycentric Julian dates
- Cadence information
- Quality flags
- Centroid positions
- Error measurements

### Current State (version-2 branch)

Recent refactoring has removed:
- CLI interface (`src/lightcurvedb/cli/`)
- Manager classes (`src/lightcurvedb/managers/`)
- Ingestor functionality

The project uses property-based testing with Hypothesis and includes extensive TESS test data including FITS files and ephemeris data from 2018-2023.

## Database Configuration

The system expects a PostgreSQL database with credentials stored in a configuration file:
```ini
[Credentials]
username=your-postgresql-username
password=your-postgresql-password
database_name=lightcurvedb
database_host=host
database_port=port
```

## Testing Approach

- Property-based testing with Hypothesis
- Test data includes real TESS observations
- Tests cover database operations, model relationships, and data integrity
- Automated testing across Python 3.9-3.12 via nox
