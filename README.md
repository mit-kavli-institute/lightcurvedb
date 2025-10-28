# Welcome to the readme

## Installation
There are a couple of ways of installing `lightcurvedb`.
1. Clone and install this repository and install using `cd lightcurvedb && pip install .`, this will install the required dependencies as well.
    a. If installing in the PDO context, the `pdo_user_install.sh` script will allow easier installation of dependencies which require C compilation.
2. Install directly through `pip`: `pip install git+https://tessgit.mit.edu/wcfong/lightcurve-database.git`. However, missing dependencies will require manual installation.

#### Configuration
`lightcurvedb` requires a connection to a PostgreSQL database. To avoid requiring users to put sensitive credentials through interactive Python shells or command line arguments,
`lightcurvedb` elects to use configuration files as a means to safeguard usernames and passwords (so long as those files have the proper access permissions).

`lightcurvedb` reads configuration files by default in `~/.config/lightcurvedb/db.conf` although this can be overridden at runtime. The configuration structure is as follows:
```
[Credentials]
username=your-postgresql-username
password=your-postgresql-password
database_name=lightcurvedb
database_host=host
database_port=port
```

Ask your system administrators for credentials and other configuration parameters if you are not hosting the database yourself.

### Quick use
As a demonstration of `lightcurvedb`, let's query lightcurves using the
SQLAlchemy ORM.
```python
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from lightcurvedb.models import Target, DataSet

# Create database session
engine = create_engine("postgresql://...")
session = Session(engine)

# Query a target and its datasets
target = session.query(Target).filter_by(name=97700520).first()
datasets = target.datasets

# Access dataset properties
for ds in datasets:
    print(f"Photometry: {ds.photometry_source.name}")
    print(f"Processing: {ds.processing_method.name if ds.processing_method else 'Raw'}")
    print(f"Values: {ds.values}")
    print(f"Errors: {ds.errors}")
```

### DataSet Structure
Lightcurves are stored as `DataSet` objects, primarily identified by their
TIC IDs and associated with specific observations. Each dataset represents a
processed lightcurve with optional photometric source and processing method.
Multiple datasets can be produced for each target and observation, with
different combinations of photometry and processing.

| DataSet Field | Description |
| ------------- | ----------- |
| id | Primary key identifier |
| target_id | Foreign key to the Target (TIC ID) |
| observation_id | Foreign key to the Observation |
| photometric_method_id | Foreign key to PhotometricSource (nullable) |
| processing_method_id | Foreign key to ProcessingMethod (nullable) |
| values | Array of photometric measurements (flux or magnitude) |
| errors | Array of measurement uncertainties |
| source_datasets | Parent datasets this was derived from (for lineage) |
| derived_datasets | Child datasets derived from this one |

#### Dataset Hierarchy
DataSets support hierarchical relationships for tracking data lineage and
processing provenance. A raw photometry dataset can have multiple derived
datasets (e.g., different detrending methods), and a processed dataset can
reference its source datasets. This enables full traceability of data
processing pipelines.

```python
# Example: Track processing lineage
raw_dataset = session.query(DataSet).filter_by(
    target=target,
    processing_method=None
).first()

# View all derived products
for derived in raw_dataset.derived_datasets:
    print(f"Derived: {derived.processing_method.name}")
```
