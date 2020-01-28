# Welcome to the readme

### To Setup and Develop
1. Install `pipenv` using `pip`
2. Instantiate a pipenv environment using `pipenv shell`
3. While inside the environment you can run `tox` or `pytest tests` to run tests
    * Running `tox` will require that you have both a py27 and py38 environments installed
    * Running `pytest` will only test with the current virtual environment (configured to be py38)

### Changing virtualenv for development
1. Edit pipenv file and locate the python version option, edit as necessary
2. Adding or removing packages should be done via `pipenv install` or `pipenv uninstall` and
not through `pip` as `pip` will not update the `Pipenv.lock` file.
