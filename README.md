# Welcome to the readme

### To Setup and Develop
1. Install `pipenv` using `pip`
2. Instantiate a pipenv environment using `pipenv shell`
3. While inside the environment you can run `tox` or `pytest tests` to run tests
    * Running `tox` will require that you have both py27 and py38 environments installed
    * Running `pytest` will only test with the current virtual environment (configured to be py38)
    * See `tox.ini` file for test configuration
4. Ensure that the directory `~/.config/lightcurvedb/` exists with a configuration file `db.conf`
    * There should be a `[Credentials]` subfield with the values
        1. `username=psql-username`
        2. `password=psswrd`
        3. `database_name=lightcurve`
        4. `database_host=filesystem.domain.com`
        5. `database_port=5432`

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

## Development workflow
All edits should be done on a separate branch from master with a name relevant to the set of changes.
During the development of the branch, any relevant tests should be placed in the `tests` directory.

1. Create a branch for your changes `git branch -b [your-branch-name]`
2. Make your changes and create any relevant tests in `tests`
    * Pytest discovers tests with files with the name schema: `test_filename.py`
    * Pytest discovers test functions with the name `def test_foo(arg1, ...): bar`
    * Remember `tox` will run all test functions through python 2 and 3
3. After review make a `merge request` for code review and final merging with master