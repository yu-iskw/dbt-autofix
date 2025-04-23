# dbt-cleanup

This tool can help teams clean up their dbt projects so that it conforms with dbt configuration best practices:

- it removes duplicate keys in YAML files and only keep the last one
  - this should not change the behavior of the current project
- it refactors YAML file to follow config best practices
  - it moves all the allowed configs under `config`
  - it moves all the custom configs under `meta`, and `meta` under `config`
  - the tool tries to keep most YAML comments intact as part of the refactor
- it refactors SQL files, removing some invaid Jinja
  - it removes `{% endmacro %}` and `{% endif %}` that don't have corresponding opening statements

## Installation

To run it from the git repo directly, install `uv` and then

run the tool directly
```sh
uvx --from git+https://github.com/dbt-labs/dbt-cleanup.git dbt-cleanup --help
```

or install it so that it can be run with `dbt-cleanup` in the future
```sh
uv tool install --from git+https://github.com/dbt-labs/dbt-cleanup.git dbt-cleanup
```

## Usage

- `dbt-cleanup refactor`: refactor YAML files following config best practices
  - add `--path <mypath>` to configure the path of the dbt project (defaults to `.`)
  - add `--dry-run` for running in dry run mode
  - add `--json` to get resulting data in a JSONL format

Each JSON object will have the following keys:

- "mode": "applied" or "dry_run" 
- "file_path": the full path of the file modified. Each file will appear only once
- "refactors": the list of refactoring rules applied

Calling `refactor` without `--dry-run` should be safe if your dbt code is part of a git repo. 

Please review the suggested changes to your dbt project before merging to `main` and make those changes go through your typical CI/CD process.
