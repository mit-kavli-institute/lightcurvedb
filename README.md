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
As a demonstration of `lightcurvedb`, let's get the full lightcurve for `97700520`.
```python
from lightcurvedb import BestLightcurveManager

lm = BestLightcurveManager

lc = lm[97700520]  # Queries are created and executed automatically by keying LightcurveManager instances
lc  # A structured numpy array

# Access the named properties of the returned array
lc["data"]  # Magnitude/Flux values
lc["cadences"]  # Ordered cadence values
lc["barycentric_julian_dates"]  # Time corrected BJD values.
```

### Lightcurve Structure
Lightcurves are stored as inline-arrays per orbit, primarily identified by their TIC ids. For each star and orbit, multiple timeseries are produced. One for each Aperture in `qlp` and
for every detrending algorithm used, including the raw photometric lightcurve without any detrending. Each timeseries (orbit lightcurve) has these fields defined:

| Lightcurve Field      | Description |
| ----------- | ----------- |
| tic_id      | The host star's TIC identifier       |
| camera   | Which camera produced this timeseries        |
| ccd | Which CCD produced this timeseries |
| orbit_id | A foreign key to the Orbit row which this lightcurve was recorded in |
| aperture_id | A foreign key to the Aperture row which was used |
| lightcurve_type_id | A foreign key to the detrending method used (or the raw photometric lightcurve) |
| cadences | The TESS cadences used in the lightcurve |
| barycentric_julian_dates | The barycenter corrected times for each exposure |
| data | The actual magnitude / flux values |
| errors | The error for the data field |
| x_centroids | The _flux_ weighted aperture centroid (x axis)* |
| y_centroids | The _flux_ weighted aperture centroid (y axis)* |
| quality_flags | The quality flag field |
| ------------- | ----------------------- |

* For centroid values without any flux-weights, use the `Background` lightcurve type.

All array fields (barycentric times, magnitude/flux values and errors, centroids and quality flags) are ordered by the cadences field.
