from pathlib import Path

import typer
from typing_extensions import Annotated

from dbt_cleanup.duplicate_keys import find_duplicate_keys, print_duplicate_keys
from dbt_cleanup.refactor import apply_changesets, changeset_all_yml_files

app = typer.Typer()


@app.command(name="duplicates")
def identify_duplicate_keys(path: Path = Path(".")):
    print(f"Identifying duplicates in {path}")
    project_duplicates, package_duplicates = find_duplicate_keys(path)
    print_duplicate_keys(project_duplicates, package_duplicates)


@app.command(name="refactor")
def refactor_yml(
    path: Path = Path("."),
    dry_run: Annotated[bool, typer.Option("--dry-run", "-d")] = False,
):
    changesets = changeset_all_yml_files(path)
    if dry_run:
        for changeset in changesets:
            print(changeset.refactored_yaml)
    else:
        apply_changesets(changesets)


if __name__ == "__main__":
    app()
