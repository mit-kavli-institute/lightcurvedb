import nox


@nox.session(python=["3.9", "3.10", "3.11", "3.12"])
def property_tests(session):
    spec = nox.project.load_toml("pyproject.toml")
    project_requirements = spec["project"]["dependencies"]
    test_requirements = spec["project"]["optional-dependencies"]["dev"]
    session.install(*project_requirements)
    session.install(*test_requirements)
    session.run("pytest")
