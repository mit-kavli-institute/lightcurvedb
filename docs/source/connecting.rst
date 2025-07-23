Connecting to the Database
==========================

Creating connection instance
############################
All connection IO is served through a `lightcurvedb.core.connection.DB`
instance. There are a couple of methods of instantiating a ``DB`` instance.

*********************
Default configuration
*********************
Your default configuration is expected to be at
``~/.config/lightcurvedb/db.conf``. One of the easier forms of getting a
database class is by:

.. code-block:: python
    :linenos:

    from lightcurvedb import db

Where ``db`` is an instantiated database connection object.

************************
Overriding configuration
************************
Sometimes you might want to connect to a different database or provide
different runtime connection parameters. *The default ``db`` object does not
provide this functionality*.

Instead ``lightcurvedb`` exposes the factory function ``db_from_config`` which
allows specification of different configuration files or runtime
overrides/parameters.

.. code-block:: python
    :linenos:

    from lightcurvedb import db_from_config

    # Equivalent to previous example
    db = db_from_config()

    # Or you can override configs
    db = db_from_config(config_path="/path/to/config.conf")

    # You can even provide user-relative paths
    db = db_from_config(config_path="~/some/user/relativepath.conf")


Opening a Connection
####################
Obtaining database instances will return a ``DB`` object in a closed state.
There are a few ways to open and close connections to the database.


*************
Declaratively
*************
Connections may be manually opened and closed via:

.. code-block:: python
    :linenos:

    db.open()
    # Perform queries, inserts, updates, deletions, etc
    foo()

    db.close()

In this manner, users are expected to open and close their connections
responsibly. Failure to do so might result in their connections timing out
due to administrator watch-dog processes.


***********
Contextuals
***********
Maintaining manual connections can be cumbersome, especially when taking into
consideration that exceptions may arise and other runtime effects. Generally
it's best to allow python to manage cleaning up resources in a clear manner.

This is accomplished using the ``with`` python block.

.. code-block:: python
    :linenos:

    with db as open_db:
        open_db.foo()
        # Other commands...
    # db is now closed

You may also short-hand this a little further with:

.. code-block:: python
    :linenos:

    with db:
        db.foo()

Existing the ``with`` block will always free the resource. Whether that reason
is reaching the end of the block, or an exception being raised somewhere
within the block, or even a ``return`` statement.

Functional Wrappers
###################
``with`` blocks are fine until you notice your code starting to have major
indented blocks.

.. code-block:: python
    :linenos:

    with db:
        if something:
            for x in array:
                db.add(x)
        models = (
            db
            .query(Model)
            .filter_by(foo=bar)
            .limit(20)
            .all()
        )
        for model in models:
            print(model)
            model.foo = "not bar"
        db.commit()

Everywhere inside the block needs an open connection. So all code is indented
as to be syntactically inside the block. One could get around this by defining
the code-block inside a function.

.. code-block:: python
    :linenos:

    def operation(db):
        if something:
            for x in array:
                db.add(x)
        models = (
            db
            .query(Model)
            .filter_by(foo=bar)
            .limit(20)
            .all()
        )
        for model in models:
            print(model)
            model.foo = "not bar"
        db.commit()

    # ...
    with db:
        operation(db)


Which is arguably more DRY, you can call this function on any open
database connection. But could still result in errors if called
without an active connection.

So ``lightcurvedb`` defines a decorator which always gives the wrapped
function an open database session.

.. code-block:: python
    :linenos:

    from lightcurvedb.io.pipeline import db_scope

    @db_scope()
    def operation(session):
        if something:
            for x in array:
                session.add(x)
        models = (
            session
            .query(Model)
            .filter_by(foo=bar)
            .limit(20)
            .all()
        )
        for model in models:
            print(model)
            model.foo = "not bar"
        session.commit()

    # ...
    operation()

The ``db_scope()`` decorator automatically provides an open database session as
the first positional argument to the wrapped function. The session is properly
closed when the function returns, with automatic rollback of any uncommitted
changes.

By default, ``db_scope`` uses the global ``LCDB_Session`` sessionmaker, but you
can provide your own:

.. code-block:: python
    :linenos:

    from sqlalchemy.orm import sessionmaker
    from lightcurvedb.io.pipeline import db_scope

    # Create a custom sessionmaker
    custom_session = sessionmaker(bind=my_engine)

    @db_scope(session_factory=custom_session)
    def custom_operation(session):
        return session.query(Model).all()

You can also pass additional arguments to the session factory:

.. code-block:: python
    :linenos:

    @db_scope(info={"task": "data_export"})
    def export_operation(session):
        # session.info contains {"task": "data_export"}
        return session.query(Model).all()

The decorator logs the function name for tracking purposes. You can override
this with ``db_scope(application_name="custom_name")`` for special cases.
