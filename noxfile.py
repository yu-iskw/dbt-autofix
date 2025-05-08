import nox

nox.options.default_venv_backend = "uv|venv"


@nox.session(python=["3.9", "3.10", "3.11", "3.12"])
def run_cli(session):
    """Make sure the CLI runs correctly"""
    session.install(".[test]")
    session.run("dbt-autofix", "--help")


@nox.session(python=["3.9", "3.10", "3.11", "3.12"])
def pytest(session):
    """Run the tests"""
    session.install(".[test]")
    session.run("pytest")
