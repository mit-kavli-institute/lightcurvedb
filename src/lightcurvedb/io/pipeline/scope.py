"""Database scope decorators for automatic session management.

This module provides decorators that simplify database session handling
by automatically managing connection lifecycles, reducing boilerplate code
and encouraging clean separation of business logic from database operations.
"""

from contextlib import contextmanager
from functools import wraps
from typing import Any, Callable, List, Optional, TypeVar

from loguru import logger
from sqlalchemy.exc import InternalError
from sqlalchemy.orm import Session
from sqlalchemy.orm.session import sessionmaker as SessionMaker

from lightcurvedb.core.connection import LCDB_Session

F = TypeVar("F", bound=Callable[..., Any])


def db_scope(
    session_factory: Optional[SessionMaker] = None,
    application_name: Optional[str] = None,
    **session_kwargs: Any,
) -> Callable[[F], F]:
    """Decorator that provides automatic database session management.

    Wraps a function to automatically handle database session lifecycle.
    The decorated function receives an open database session as its first
    argument. The session is automatically closed when the function returns,
    with automatic rollback of uncommitted changes.

    Parameters
    ----------
    session_factory : sqlalchemy.orm.sessionmaker, optional
        A SQLAlchemy sessionmaker instance for creating database sessions.
        If not provided, defaults to the global LCDB_Session.
    application_name : str, optional
        Name used for logging purposes to identify the calling function.
        If not provided, uses the wrapped function's name.
    **session_kwargs : dict
        Additional keyword arguments passed to the session factory when
        creating new sessions. Common uses include:

        - ``bind``: Override the database engine
        - ``info``: Attach metadata to the session
        - ``expire_on_commit``: Control object expiration behavior

    Returns
    -------
    Callable
        A decorator function that wraps the target function with
        automatic session management.

    Notes
    -----
    - The session is created using the provided factory with any kwargs
    - The session is automatically closed after function execution
    - Any uncommitted changes are automatically rolled back
    - The session is properly closed even if exceptions occur
    - The wrapped function must accept a session as its first argument

    Examples
    --------
    Basic usage with default session factory:

    >>> from lightcurvedb.io.pipeline import db_scope
    >>> from lightcurvedb.models import Mission
    >>>
    >>> @db_scope()
    ... def count_missions(session):
    ...     return session.query(Mission).count()
    >>>
    >>> mission_count = count_missions()

    Using a custom session factory:

    >>> from sqlalchemy.orm import sessionmaker
    >>> custom_factory = sessionmaker(bind=my_engine)
    >>>
    >>> @db_scope(session_factory=custom_factory)
    ... def custom_query(session):
    ...     return session.execute("SELECT 1").scalar()

    Passing session configuration:

    >>> @db_scope(info={"task": "data_export"})
    ... def export_data(session, table_name):
    ...     # session.info will contain {"task": "data_export"}
    ...     return session.query(table_name).all()

    See Also
    --------
    lightcurvedb.core.connection.LCDB_Session : Default session factory
    lightcurvedb.core.connection.db_from_config : Create sessions from config
    """

    def _internal(func):
        # Use provided session factory or default to LCDB_Session
        _session_factory = (
            session_factory if session_factory is not None else LCDB_Session
        )

        # Set application name for PostgreSQL connection tracking
        app_name = application_name if application_name else func.__name__

        # Prepare session kwargs
        session_creation_kwargs = session_kwargs.copy()

        @wraps(func)
        def wrapper(*args, **kwargs):
            func_results = None
            # Create a new session using the factory with provided kwargs
            logger.trace(
                f"Entering db context for {app_name} ({func}) "
                f"with {args} and {kwargs}"
            )
            with _session_factory(**session_creation_kwargs) as session:
                func_results = func(session, *args, **kwargs)
            logger.trace(f"Exited db context for {app_name} ({func})")
            return func_results

        return wrapper

    return _internal


@contextmanager
def scoped_block(
    db: Session,
    resource: Any,
    acquire_actions: Optional[List[Any]] = None,
    release_actions: Optional[List[Any]] = None,
) -> Any:
    if acquire_actions is None:
        acquire_actions = []
    if release_actions is None:
        release_actions = []

    try:
        for action in acquire_actions:
            logger.trace(action)
            db.execute(action)
        db.commit()
        yield resource
    except InternalError:
        db.rollback()
    finally:
        for action in release_actions:
            logger.trace(action)
            db.execute(action)
        db.commit()
