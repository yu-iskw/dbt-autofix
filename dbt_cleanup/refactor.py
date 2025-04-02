import difflib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

from dbt_meshify.storage.file_manager import DbtYAML, YAMLFileManager
from rich.console import Console
from ruamel.yaml.constructor import DuplicateKeyError

console = Console()

yaml_file_manager = YAMLFileManager()


def output_yaml(content: Dict) -> str:
    """Write a dict value to a YAML string"""
    yaml = DbtYAML()
    clean_content = YAMLFileManager._clean_content(content)
    file_text = yaml.dump(clean_content)
    return file_text


def load_yaml_check_duplicates(yml_file: Path) -> str:
    try:
        data = yaml_file_manager.read_file(yml_file)
    except DuplicateKeyError:
        console.print(
            f"There are duplicate keys in {yml_file.absolute()}\nTo identify all of those, run the 'duplicates' command.\nMake sure to fix those before re-running the refactor command.",
            style="bold red",
        )
        exit(1)
    except Exception as e:
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
    "tests",
    "columns",
    "time_spine",
    "versions",
]


@dataclass
class RefactorResult:
    file_path: Path
    refactored: bool
    refactored_yaml: bool
    original_yaml: str
    refactor_logs: list[str]

    def update_yaml_file(self) -> None:
        """Update the YAML file with the refactored content"""
        Path(self.file_path).write_text(self.refactored_yaml)


def changeset_refactor_yml(yml_file: Path) -> RefactorResult:
    """Generates a refactored YAML string from a single YAML file
    - moves all the config fields under config
    - moves all the meta fields under config.meta and merges with existing config.meta
    - moves all the unknown fields under config.meta
    - provide some information if some fields don't exist but are similiar to allowed fields
    """
    refactored = False
    refactor_logs = []
    data = load_yaml_check_duplicates(yml_file)

    for model in data.get("models", []):
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
                model["config"] = model_config
                del model[field]
            if field not in allowed_config_fields:
                refactored = True
                closest_match = difflib.get_close_matches(field, allowed_config_fields, 1)
                if closest_match and closest_match[0] in allowed_config_fields:
                    refactor_logs.append(
                        f"Field '{field}' is not allowed, but '{closest_match[0]}' is. We moved it under config.meta but you might want to rename it and move it under config."
                    )
                model_meta = model.get("config", {}).get("meta", {})
                model_meta.update({field: model[field]})
                model["config"] = {"meta": model_meta}
                del model[field]

        if existing_meta:
            refactored = True
            if "config" not in model:
                model["config"] = {"meta": {}}
            if "meta" not in model["config"]:
                model["config"]["meta"] = {}
            for key, value in existing_meta.items():
                model["config"]["meta"].update({key: value})
            del model["meta"]

    return RefactorResult(
        file_path=yml_file,
        refactored=refactored,
        refactored_yaml=output_yaml(data),
        original_yaml=output_yaml(data),
        refactor_logs=refactor_logs,
    )


def changeset_all_yml_files(path: Path) -> list[RefactorResult]:
    models_path = yaml_file_manager.read_file(path / "dbt_project.yml").get(
        "model-paths", ["models"]
    )

    refactor_results = []
    for model_path in models_path:
        yaml_files = set((path / Path(model_path)).resolve().glob("**/*.yml")).union(
            set((path / Path(model_path)).resolve().glob("**/*.yaml"))
        )
        for yml_file in yaml_files:
            refactor_result = changeset_refactor_yml(yml_file)
            refactor_results.append(refactor_result)
    return refactor_results


def apply_changesets(changesets: list[RefactorResult]) -> None:
    """Apply the changesets to the YAML files"""
    for changeset in changesets:
        if changeset.refactored:
            changeset.update_yaml_file()
