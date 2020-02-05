# Welcome to the readme

### To Setup and Develop
1. Install `pipenv` using `pip`
2. Instantiate a pipenv environment using `pipenv shell`
3. While inside the environment you can run `tox` or `pytest tests` to run tests
    * Running `tox` will require that you have both py27 and py38 environments installed
    * Running `pytest` will only test with the current virtual environment (configured to be py38)
    * See `tox.ini` file for test configuration

### Changing virtualenv for development
1. Edit pipenv file and locate the python version option, edit as necessary
2. Adding or removing packages should be done via `pipenv install` or `pipenv uninstall` and
not through `pip` as `pip` will not update the `Pipenv.lock` file.

### Deployment
1. Ensure postgresql (>= 9.6) is installed
2. Create a database (e.g `lightcurve`)
3. Install `lightcurvedb` using `pip install .`
4. Copy `alembic.ini.example` to `alembic.ini`
    1. Configure the `alembic.ini` field: `sqlalchemy.url` to point to the url of the database
    2. Run `alembic upgrade head` to run migrations
5. Ingest wanted data using provided CLI tools