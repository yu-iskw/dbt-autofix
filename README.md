# dbt-cleanup

This tool can help teams clean up their dbt projects so that it conforms with dbt configuration best practices:

- it helps identify duplicate keys in YAML
  - while dbt allows those today, it is not allowed in YAML normally
  - dbt also takes in consideration the last key in case of duplicates which is not the intended behavior from the YAML spec in case of duplicates
- it helps refactor YAML file to follow config best practices
  - it moves all the allowed configs under `config`
  - it moves all the custom configs under `meta`, and `meta` under `config`
  - the tool tries to keep most YAML comments intact as part of the refactor
- it helps refactor SQL files, removing some invaid Jinja
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

- `dbt-cleanup duplicates`: identify duplicate keys in YAML and prompt users to fix those manually
  - add `--path <mypath>` to configure the path of the dbt project (defaults to `.`)
- `dbt-cleanup refactor`: refactor YAML files following config best practices
  - add `--path <mypath>` to configure the path of the dbt project (defaults to `.`)
  - add `--dry-run` for running in dry run mode

Calling `refactor` without `--dry-run` should be safe if your dbt code is part of a git repo. 

Please review the changes before merging to `main` and maket those changes go through your typical CI/CD process.
