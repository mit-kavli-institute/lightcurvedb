************
Installation
************

Via Pip and Git
***************
Run your preferred version of Pip using Git and install LightcurveDB with

.. code-block:: console

    pip install git+https://tessgit.mit.edu/wcfong/lightcurve-database.git

This will install the latest version.

Manual Install
**************
When doing development and you need to install from source you can split
apart the Git clone and Pip install steps.

.. code-block:: console

    git clone git@tessgit.mit.edu:wcfong/lightcurve-database.git
    cd lightcurve-database
    pip install .

PDO Context
***********
*This usually only needs to be done once*
PDO does not provide needed C/C++ shared object files for `psycopg2`. In order
to install libraries needed for efficient lightcurve ingestion an extra step
needs to be done.

You will need pdodev login permissions in order to perform these steps.

Manual PDO Install
==================
.. code-block:: console

    ssh pdodev
    cd ./where-you-git-cloned/lightcurve-database
    pip install .
    exit

For ease of initial install or addition of a python package that requires
dynamic C/C++ libraries you may also use the `pdo_user_install.sh` bash
script.

.. code-block:: console

   bash pdo_user_install.sh {Python Major Version, defaults to 3}


Extras Installation
*******************
If you wish to install the documentation dependencies
(to build the docs yourself) you can use pip.

.. code-block:: console

    pip install ".[docs]"
