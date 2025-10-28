Welcome to LightcurveDB's documentation!
========================================

LightcurveDB is a PostgreSQL-backed system for storing and retrieving astronomical time-series data from the TESS (Transiting Exoplanet Survey Satellite) mission.

Installation
############
To install the latest version from GitHub:

.. code-block:: console

    pip install git+https://github.com/mit-kavli-institute/lightcurvedb.git

See other :doc:`installation <installation>` steps for specific computer contexts.

Development
###########
Clone the repository and set up the development environment:

.. code-block:: console

    git clone https://github.com/mit-kavli-institute/lightcurvedb.git
    cd lightcurvedb
    pip install -e ".[dev]"

The project uses ``pyproject.toml`` for dependency management. Development dependencies include testing, linting, and documentation tools.

Docker Development
~~~~~~~~~~~~~~~~~~
A Docker environment is provided for consistent development:

.. code-block:: console

    docker-compose up -d
    docker-compose exec tester bash

Testing
#######
The project uses ``nox`` for test automation across Python versions (3.11-3.12). Tests require a PostgreSQL database.

**Local testing:**

.. code-block:: console

    nox  # Run all test sessions
    pytest  # Run tests directly

**Docker testing:**

.. code-block:: console

    docker-compose exec tester nox

Tests use pytest-xdist for parallel execution with per-worker database isolation.

Continuous Integration
######################
The project uses GitHub Actions for automated testing and deployment:

* **Tests**: Run automatically on all pushes and pull requests across Python 3.11-3.12
* **Documentation**: Built and deployed to GitHub Pages on pushes to main branches
* **Semantic Release**: Automated versioning based on commit messages

Contributing
############
This project follows the Angular commit convention for automatic semantic versioning:

* ``feat:``: New features (minor version bump)
* ``fix:``: Bug fixes (patch version bump)
* ``feat!:`` or ``BREAKING CHANGE:``: Breaking changes (major version bump)
* ``docs:``, ``style:``, ``refactor:``, ``test:``, ``chore:``: No version bump

Documentation
#############
**Build locally:**

.. code-block:: console

    nox -s docs
    # or
    sphinx-build -b html docs/source/ docs/build/html

**View online:**
Documentation is automatically published to `GitHub Pages <https://mit-kavli-institute.github.io/lightcurvedb/>`_.


Submodule Documentation
#######################
.. toctree::
    :maxdepth: 2

    installation
    connecting
    schema
    models
    db/db
    util/utils


Indices and tables
==================
* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
