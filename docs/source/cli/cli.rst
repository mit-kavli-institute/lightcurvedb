LightcurveDB CLI Documentation
==============================
Lightcurvedb provides command-line interface tools to load, delete, and query data. Keep in mind that some of the following interfaces require administrative permissions and may not be available to you.

Every ``lcdb`` command provides the `--help` flag which provides the documentation printout of the relevant usage.

Installing ``lightcurvedb`` will place the entry point for the command-line to ``lcdb``.  To use any command listed follows this general structure.

.. click:: lightcurvedb.cli.base:lcdbcli
   :prog: lcdb
   :nested: none
