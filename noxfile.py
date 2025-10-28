import nox
from nox.sessions import Session


@nox.session(python=["3.9", "3.10", "3.11", "3.12"])
def property_tests(session):
    session.install(
        "-e",
        ".[dev]",
        "--extra-index-url",
        "https://mit-kavli-institute.github.io/MIT-Kavli-PyPi/",
    )
    flags = session.posargs if session.posargs else ["-n", "auto"]
    session.run(
        "pytest",
        "--cov=lightcurvedb",
        "--cov-report=term-missing",
        *flags,
    )


@nox.session(python=["3.9"])
def docs(session: Session):
    spec = nox.project.load_toml("pyproject.toml")
    requirements = spec["project"]["optional-dependencies"]["docs"]
    # Install the package in editable mode with all dependencies
    session.install(
        ".[dev]",
        "--extra-index-url",
        "https://mit-kavli-institute.github.io/MIT-Kavli-PyPi/",
    )
    session.install(*requirements)

    session.run(
        "sphinx-build", "-b", "html", "docs/source/", "docs/build/html"
    )
