from pathlib import Path

import typer
from rich import print
from typing_extensions import Annotated

from dbt_cleanup.duplicate_keys import find_duplicate_keys, print_duplicate_keys
from dbt_cleanup.refactor import apply_changesets, changeset_all_sql_yml_files

app = typer.Typer(
    help="A tool to help clean up dbt projects",
    no_args_is_help=True,
    add_completion=False,
)


@app.command(name="duplicates")
def identify_duplicate_keys(
    path: Annotated[Path, typer.Option("--path", "-p", help="The path to the dbt project")] = Path(
        "."
    ),
):
    print(f"[green]Identifying duplicates in {path}[/green]\n")
    project_duplicates, package_duplicates = find_duplicate_keys(path)
    print_duplicate_keys(project_duplicates, package_duplicates)


@app.command(name="refactor")
def refactor_yml(
    path: Annotated[Path, typer.Option("--path", "-p", help="The path to the dbt project")] = Path(
        "."
    ),
    dry_run: Annotated[
        bool, typer.Option("--dry-run", "-d", help="In dry run mode, do not apply changes")
    ] = False,
):
    changesets = changeset_all_sql_yml_files(path)
    yaml_results, sql_results = changesets
    if dry_run:
        print("[red]-- Dry run mode, not applying changes --[/red]")
        for changeset in yaml_results:
            if changeset.refactored:
                changeset.print_to_console(dry_run)
        for changeset in sql_results:
            if changeset.refactored:
                changeset.print_to_console(dry_run)
    else:
        apply_changesets(yaml_results, sql_results)


if __name__ == "__main__":
    app()
