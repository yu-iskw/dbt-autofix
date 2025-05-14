# dbt-autofix (previously dbt-cleanup)

This tool can help teams clean up their dbt projects so that it conforms with dbt configuration best practices and update deprecated config:


| Deprecation Code in dbt Core       | Files             | Description                                                                                      |
| ---------------------------------- | ----------------- | ------------------------------------------------------------------------------------------------ |
| `CustomKeyInObjectDeprecation` (*) | YAML files        | Move all models configs under `config:` in YAML files                                            |
| `CustomKeyInObjectDeprecation` (*) | YAML files        | Move all models extra config (not valid or custom) under `meta:` and `meta` under `config:`      |
| `DuplicateYAMLKeysDeprecation`     | YAML files        | Remove duplicate keys in YAML files, keeping the second one to keep the same behaviour           |
| `UnexpectedJinjaBlockDeprecation`  | SQL files         | Remove extra `{% endmacro %}` and `{% endif %}` that don't have corresponding opening statements |
| -                                  | `dbt_project.yml` | Prefix all configs for modeles/tests etc... with a `+`                                           |
| `ConfigDataPathDeprecation`        | `dbt_project.yml` | Remove deprecated config for data path (now seed)                                                |
| `ConfigLogPathDeprecation`         | `dbt_project.yml` | Remove deprecated config for log path                                                            |
| `ConfigSourcePathDeprecation`      | `dbt_project.yml` | Remove deprecated config for source path                                                         |
| `ConfigTargetPathDeprecation`      | `dbt_project.yml` | Remove deprecated config for target path                                                         |

(*) : those autofix rules are currently deactivated in `main` 

## Installation

To run it from the git repo directly, install `uv` and then

run the tool directly
```sh
uvx --from git+https://github.com/dbt-labs/dbt-autofix.git dbt-autofix --help
```

or install it so that it can be run with `dbt-cleanup` in the future
```sh
uv tool install --from git+https://github.com/dbt-labs/dbt-autofix.git dbt-autofix
```

## Usage

- `dbt-autofix deprecations`: refactor YAML and SQL files to fix some deprecations
  - add `--path <mypath>` to configure the path of the dbt project (defaults to `.`)
  - add `--dry-run` for running in dry run mode
  - add `--json` to get resulting data in a JSONL format

Each JSON object will have the following keys:

- "mode": "applied" or "dry_run" 
- "file_path": the full path of the file modified. Each file will appear only once
- "refactors": the list of refactoring rules applied

Calling `deprecations` without `--dry-run` should be safe if your dbt code is part of a git repo. 

Please review the suggested changes to your dbt project before merging to `main` and make those changes go through your typical CI/CD process.
