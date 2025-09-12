import nox
from nox.sessions import Session


@nox.session(python=["3.11", "3.12"])
def property_tests(session):
    session.install(
        ".[dev]",
        "--extra-index-url",
        "https://mit-kavli-institute.github.io/MIT-Kavli-PyPi/",
    )
    flags = session.posargs if session.posargs else []
    session.run("pytest", *flags)


@nox.session(python=["3.11"])
def docs(session: Session):
    session.install(
        ".[docs]",
        "--extra-index-url",
        "https://mit-kavli-institute.github.io/MIT-Kavli-PyPi/",
    )
    session.run("sphinx-build", "-M", "html", "docs/source/", "docs/build/")
