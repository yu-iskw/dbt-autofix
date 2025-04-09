import difflib
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Set, Tuple

from dbt_meshify.storage.file_manager import DbtYAML, YAMLFileManager
from rich.console import Console
from ruamel.yaml.constructor import DuplicateKeyError
from yaml import safe_load

console = Console()

yaml_file_manager = YAMLFileManager()


def output_yaml(content: Dict) -> str:
    """Write a dict value to a YAML string"""
    yaml = DbtYAML()
    clean_content = YAMLFileManager._clean_content(content)
    file_text = yaml.dump(clean_content)
    return file_text


def load_yaml_check_duplicates(yml_file: Path) -> str:
    bypass_error_for_test = (os.getenv("DBT_CLEANUP_BYPASS_ERROR_FOR_TEST") or "0").lower() in [
        "true",
        "1",
        "yes",
        "y",
    ]
    try:
        data = yaml_file_manager.read_file(yml_file)
    except DuplicateKeyError:
        console.print(
            f"There are duplicate keys in {yml_file.absolute()}\nTo identify all of those, run the 'duplicates' command.\nMake sure to fix those before re-running the refactor command.",
            style="bold red",
        )
        if bypass_error_for_test:
            return {}
        exit(1)
    except Exception as e:
        if bypass_error_for_test:
            return {}
        console.print(f"Error loading {yml_file.absolute()}: {e}", style="bold red")
        exit(1)
    return data or {}


allowed_config_fields = set(
    [
        # model specific
        "materialized",
        "sql_header",
        "on_configuration_change",
        "unique_key",
        "batch_size",
        "begin",
        "lookback",
        "concurrent_batches",
        # general
        "enabled",
        "tags",
        "pre_hook",
        "post_hook",
        "database",
        "schema",
        "alias",
        "persist_docs",
        "meta",
        "grants",
        "contract",
        "event_time",
        # Snowflake: https://github.com/dbt-labs/dbt-adapters/blob/af33935b119347cc021554ea854884bce986ef8d/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L42-L58
        "transient",
        "cluster_by",
        "automatic_clustering",
        "secure",
        "copy_grants",
        "snowflake_warehouse",
        "query_tag",
        "tmp_relation_type",
        "merge_update_columns",
        "target_lag",
        "table_format",
        "external_volume",
        "base_location_root",
        "base_location_subpath",
        # BQ: https://github.com/dbt-labs/dbt-adapters/blob/af33935b119347cc021554ea854884bce986ef8d/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L98
        "dataset",
        "project",
        "cluster_by",
        "partition_by",
        "kms_key_name",
        "labels",
        "partitions",
        "grant_access_to",
        "hours_to_expiration",
        "require_partition_filter",
        "partition_expiration_days",
        "merge_update_columns",
        "enable_refresh",
        "refresh_interval_minutes",
        "max_staleness",
        "enable_list_inference",
        "intermediate_format",
        "submission_method",
        # Postgres
        "unlogged",
        "indexes",
        # Redshift
        "sort_type",
        "dist",
        "sort",
        "bind",
        "backup",
        "auto_refresh",
        # DBX: https://github.com/databricks/dbt-databricks/blob/fde68588d45e8a299a83318acdca6c97081088e8/dbt/adapters/databricks/impl.py#L105
        "file_format",
        "table_format",
        "location_root",
        "include_full_name_in_path",
        "partition_by",
        "clustered_by",
        "liquid_clustered_by",
        "auto_liquid_cluster",
        "buckets",
        "options",
        "merge_update_columns",
        "merge_exclude_columns",
        "databricks_tags",
        "tblproperties",
        "zorder",
        "unique_tmp_table_suffix",
        "skip_non_matched_step",
        "skip_matched_step",
        "matched_condition",
        "not_matched_condition",
        "not_matched_by_source_action",
        "not_matched_by_source_condition",
        "target_alias",
        "source_alias",
        "merge_with_schema_evolution",
    ]
)

allowed_config_fields_except_meta = allowed_config_fields - {"meta"}

allowed_fields = [
    "name",
    "description",
    "docs",
    "latest_version",
    "deprecation_date",
    "access",
    "config",
    "constraints",
    "data_tests",
    "tests",
    "columns",
    "time_spine",
    "versions",
]


@dataclass
class YMLRefactorResult:
    file_path: Path
    refactored: bool
    refactored_yaml: str
    original_yaml: str
    refactor_logs: list[str]

    def update_yaml_file(self) -> None:
        """Update the YAML file with the refactored content"""
        Path(self.file_path).write_text(self.refactored_yaml)

    def print_to_console(self, dry_run: bool = False):
        console.print(
            f"\n{'DRY RUN - NOT APPLIED: ' if dry_run else ''}Refactored {self.file_path}:",
            style="green",
        )
        for log in self.refactor_logs:
            console.print(f"  {log}", style="yellow")


@dataclass
class SQLRefactorResult:
    file_path: Path
    refactored: bool
    refactored_content: str
    original_content: str
    refactor_logs: list[str]

    def update_sql_file(self) -> None:
        """Update the SQL file with the refactored content"""
        Path(self.file_path).write_text(self.refactored_content)

    def print_to_console(self, dry_run: bool = False):
        console.print(
            f"\n{'DRY RUN - NOT APPLIED: ' if dry_run else ''}Refactored {self.file_path}:",
            style="green",
        )
        for log in self.refactor_logs:
            console.print(f"  {log}", style="yellow")


def remove_unmatched_endings(sql_content: str) -> Tuple[str, List[str]]:
    """Remove unmatched {% endmacro %} and {% endif %} tags from SQL content.

    Handles:
    - Multi-line tags
    - Whitespace control variants ({%- and -%})
    - Nested blocks

    Args:
        sql_content: The SQL content to process

    Returns:
        Tuple containing:
        - The processed SQL content
        - List of removal messages
    """
    # Regex patterns for Jinja tag matching
    JINJA_TAG_PATTERN = re.compile(r"{%-?\s*(.*?)\s*-?%}", re.DOTALL)
    MACRO_START = re.compile(r"macro\s+([^\s(]+)")  # Captures macro name
    IF_START = re.compile(r"if\s+.*")
    MACRO_END = re.compile(r"endmacro")
    IF_END = re.compile(r"endif")

    logs = []
    # Track macro and if states with their positions
    macro_stack = []  # [(start_pos, end_pos, macro_name), ...]
    if_stack = []  # [(start_pos, end_pos), ...]

    # Track positions to remove
    to_remove = []  # [(start_pos, end_pos), ...]

    # Find all Jinja tags
    for match in JINJA_TAG_PATTERN.finditer(sql_content):
        tag_content = match.group(1)
        start_pos = match.start()
        end_pos = match.end()

        # Check for macro start
        macro_match = MACRO_START.match(tag_content)
        if macro_match:
            macro_name = macro_match.group(1)
            macro_stack.append((start_pos, end_pos, macro_name))
            continue

        # Check for if start
        if IF_START.match(tag_content):
            if_stack.append((start_pos, end_pos))
            continue

        # Handle endmacro
        if MACRO_END.match(tag_content):
            if not macro_stack:
                to_remove.append((start_pos, end_pos))
                # Count lines, adjusting for content before first newline
                prefix = sql_content[:start_pos]
                first_newline = prefix.find("\n")
                if first_newline == -1:
                    line_num = 1
                else:
                    line_num = prefix.count("\n", first_newline) + 1
                logs.append(f"Removed unmatched {{% endmacro %}} near line {line_num}")
            else:
                macro_stack.pop()
            continue

        # Handle endif
        if IF_END.match(tag_content):
            if not if_stack:
                to_remove.append((start_pos, end_pos))
                # Count lines, adjusting for content before first newline
                prefix = sql_content[:start_pos]
                first_newline = prefix.find("\n")
                if first_newline == -1:
                    line_num = 1
                else:
                    line_num = prefix.count("\n", first_newline) + 1
                logs.append(f"Removed unmatched {{% endif %}} near line {line_num}")
            else:
                if_stack.pop()

    # Remove the unmatched tags from end to start to maintain correct positions
    result = sql_content
    for start, end in sorted(to_remove, reverse=True):
        result = result[:start] + result[end:]

    return result, logs


def process_yaml_files(path: Path, model_paths: List[str]) -> List[YMLRefactorResult]:
    """Process all YAML files in the project

    Args:
        path: Project root path
    """
    yaml_results = []
    for model_path in model_paths:
        yaml_files = set((path / Path(model_path)).resolve().glob("**/*.yml")).union(
            set((path / Path(model_path)).resolve().glob("**/*.yaml"))
        )
        for yml_file in yaml_files:
            refactor_result = changeset_refactor_yml(yml_file)
            yaml_results.append(refactor_result)

    return yaml_results


def process_sql_files(path: Path, sql_paths: Set[str]) -> List[SQLRefactorResult]:
    """Process all SQL files in the given paths for unmatched endings.

    Args:
        path: Base project path
        sql_paths: Set of paths relative to project root where SQL files are located

    Returns:
        List of SQLRefactorResult for each processed file
    """
    results = []

    for sql_path in sql_paths:
        full_path = (path / sql_path).resolve()
        if not full_path.exists():
            console.print(f"Warning: Path {full_path} does not exist", style="yellow")
            continue

        sql_files = full_path.glob("**/*.sql")
        for sql_file in sql_files:
            try:
                content = sql_file.read_text()
                new_content, logs = remove_unmatched_endings(content)

                results.append(
                    SQLRefactorResult(
                        file_path=sql_file,
                        refactored=new_content != content,
                        refactored_content=new_content,
                        original_content=content,
                        refactor_logs=logs,
                    )
                )
            except Exception as e:
                console.print(f"Error processing {sql_file}: {e}", style="bold red")

    return results


def restructure_yaml_keys(model: Dict) -> Tuple[Dict, bool, List[str]]:
    """Restructure YAML keys according to dbt conventions.

    Args:
        model: The model dictionary to process
        refactor_logs: List to append logs to

    Returns:
        Tuple containing:
        - The processed model dictionary
        - Boolean indicating if changes were made
        - List of refactor logs
    """
    refactored = False
    refactor_logs = []
    existing_meta = model.get("meta", {}).copy()

    # we can not loop model and modify it at the same time
    copy_model = model.copy()

    for field in copy_model:
        if field in allowed_fields:
            continue
        if field in allowed_config_fields_except_meta:
            refactored = True
            model_config = model.get("config", {})
            model_config.update({field: model[field]})
            refactor_logs.append(f"Field '{field}' would be moved under config.")
            model["config"] = model_config
            del model[field]
        if field not in allowed_config_fields:
            refactored = True
            closest_match = difflib.get_close_matches(
                field, allowed_config_fields.union(set(allowed_fields)), 1
            )
            if closest_match:
                refactor_logs.append(
                    f"Model {model['name']} - Field '{field}' is not allowed, but '{closest_match[0]}' is. Moved as-is under config.meta but you might want to rename it and move it under config."
                )
            else:
                refactor_logs.append(
                    f"Model {model['name']} - Field '{field}' is not an allowed config - Moved under config.meta."
                )
            model_meta = model.get("config", {}).get("meta", {})
            model_meta.update({field: model[field]})
            model["config"] = {"meta": model_meta}
            del model[field]

    if existing_meta:
        refactored = True
        refactor_logs.append(
            f"Model {model['name']} - Moved all the meta fields under config.meta and merged with existing config.meta."
        )
        if "config" not in model:
            model["config"] = {"meta": {}}
        if "meta" not in model["config"]:
            model["config"]["meta"] = {}
        for key, value in existing_meta.items():
            model["config"]["meta"].update({key: value})
        del model["meta"]

    return model, refactored, refactor_logs


def changeset_refactor_yml(yml_file: Path) -> YMLRefactorResult:
    """Generates a refactored YAML string from a single YAML file
    - moves all the config fields under config
    - moves all the meta fields under config.meta and merges with existing config.meta
    - moves all the unknown fields under config.meta
    - provide some information if some fields don't exist but are similar to allowed fields
    """
    refactored = False
    refactor_logs = []
    data = load_yaml_check_duplicates(yml_file)

    if "models" in data:
        for i, model in enumerate(data["models"]):
            processed_model, model_refactored, model_refactor_logs = restructure_yaml_keys(model)
            if model_refactored:
                refactored = True
                data["models"][i] = processed_model
                refactor_logs.extend(model_refactor_logs)

    return YMLRefactorResult(
        file_path=yml_file,
        refactored=refactored,
        refactored_yaml=output_yaml(data),
        original_yaml=output_yaml(data),
        refactor_logs=refactor_logs,
    )


def get_dbt_paths(path: Path) -> Tuple[List[str], List[str]]:
    """Get model and macro paths from dbt_project.yml

    Args:
        path: Project root path

    Returns:
        Tuple containing:
        - List of model paths
        - List of macro paths
    """

    with open(path / "dbt_project.yml", "r") as f:
        project_config = safe_load(f)
    model_paths = project_config.get("model-paths", ["models"])
    macro_paths = project_config.get("macro-paths", ["macros"])
    return model_paths, macro_paths


def changeset_all_sql_yml_files(
    path: Path,
) -> Tuple[List[YMLRefactorResult], List[SQLRefactorResult]]:
    """Process all YAML files and SQL files in the project

    Args:
        path: Project root path

    Returns:
        Tuple containing:
        - List of YAML refactor results
        - List of SQL refactor results
    """
    model_paths, macro_paths = get_dbt_paths(path)

    # Process SQL files
    sql_paths = set(model_paths + macro_paths)
    sql_results = process_sql_files(path, sql_paths)

    # Process YAML files
    yaml_results = process_yaml_files(path, model_paths)

    return yaml_results, sql_results


def apply_changesets(
    yaml_results: List[YMLRefactorResult], sql_results: List[SQLRefactorResult]
) -> None:
    """Apply both YAML and SQL refactoring changes

    Args:
        yaml_results: List of YAML refactoring results
        sql_results: List of SQL refactoring results
    """
    # Apply YAML changes
    for result in yaml_results:
        if result.refactored:
            result.update_yaml_file()
            result.print_to_console()

    # Apply SQL changes
    for result in sql_results:
        if result.refactored:
            result.update_sql_file()
            result.print_to_console()
