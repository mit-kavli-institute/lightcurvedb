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
    requirements = spec["project"]["optional-dependencies"]["docs"]
    # Install the package in editable mode with all dependencies
    session.install("-e", ".")
    session.install(*requirements)

    session.run(
        "sphinx-build", "-b", "html", "docs/source/", "docs/build/html"
    )


@nox.session
def generate_schema(session: Session):
    """Generate database schema diagram using Paracelsus."""
    import pathlib

    # Install the package and paracelsus
    session.install("-e", ".")
    session.install("paracelsus")

    # Ensure the output directory exists
    output_dir = pathlib.Path("docs/source/_static")
    output_dir.mkdir(exist_ok=True)

    # Generate the mermaid diagram to a separate file
    session.run(
        "paracelsus",
        "graph",
        "lightcurvedb.core.base_model:LCDBModel",
        "--import-module",
        "lightcurvedb.models.frame",
        "--import-module",
        "lightcurvedb.models.instrument",
        "--import-module",
        "lightcurvedb.models.observation",
        "--import-module",
        "lightcurvedb.models.dataset",
        "--import-module",
        "lightcurvedb.models.target",
        "--import-module",
        "lightcurvedb.models.quality_flag",
        "-o",
        "docs/source/_static/schema_auto_generated.mmd",
    )

    output_file = "docs/source/_static/schema_auto_generated.mmd"
    session.log(f"Generated schema diagram at {output_file}")
