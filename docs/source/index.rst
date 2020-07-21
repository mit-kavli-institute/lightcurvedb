Welcome to LightcurveDB's documentation!
========================================

Installation
############
Clone the repository onto the target machine using ``git``. And using the
desired ``pip`` install the package by entering into the project's root
directory and running ``pip install .``

Development
###########
Clone the repository into the desired location. Grab pipenv from ``PyPi``
using ``pip install pipenv``. Although it is most desirable to install this
package from your machine's package manager.

Create and enter a virtual environment for development by executing 
``pipenv shell``. All the dependencies will be installed.

To add a dependency run ``pipenv install [PACKAGE]``.
To add a development dependency run ``pipenv install --development [PACKAGE]``

Testing
#######
First ensure that your user can instantiate a PostgreSQL database instance.
A new database is needed per test run. This new database is torn down after
the tests are finished or encounter some python side error.

Ensure you're within a ``pipenv`` environemtn and run ``tox``.

.. toctree::
    :maxdepth: 2
    :caption: Contents:

    db/db
    lightcurves/lightcurves
    managers/managers


Indices and tables
==================
* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
