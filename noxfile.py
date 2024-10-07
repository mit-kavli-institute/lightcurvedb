import nox
from nox.sessions import Session


@nox.session(python=["3.9", "3.10", "3.11", "3.12"])
def property_tests(session):
    session.install(".[dev]")
    flags = session.posargs if session.posargs else []
    session.run("pytest", *flags)


@nox.session(python=["3.9"])
def docs(session: Session):
    spec = nox.project.load_toml("pyproject.toml")
    project_requirements = spec["project"]["dependencies"]
    requirements = spec["project"]["optional-dependencies"]["docs"]
    session.install(*project_requirements)
    session.install(*requirements)

    session.run("sphinx-build", "-M", "html", "docs/source/", "docs/build/")
