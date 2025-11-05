import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import yamllint.config

from dbt_autofix.refactors.results import DbtDeprecationRefactor, YMLRuleRefactorResult
from dbt_autofix.refactors.yml import DbtYAML
from dbt_autofix.retrieve_schemas import DbtProjectSpecs, SchemaSpecs

config = """
rules:
  key-duplicates: enable
"""

yaml_config = yamllint.config.YamlLintConfig(config)


def changeset_dbt_project_remove_deprecated_config(
    yml_str: str, exclude_dbt_project_keys: bool = False
) -> YMLRuleRefactorResult:
    """Remove deprecated keys"""
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []

    dict_deprecated_fields_with_defaults = {
        "log-path": "logs",
        "target-path": "target",
    }

    dict_renamed_fields = {
        "data-paths": "seed-paths",
        "source-paths": "model-paths",
    }

    dict_fields_to_deprecation_class = {
        "log-path": "ConfigLogPathDeprecation",
        "target-path": "ConfigTargetPathDeprecation",
        "data-paths": "ConfigDataPathDeprecation",
        "source-paths": "ConfigSourcePathDeprecation",
    }

    yml_dict = DbtYAML().load(yml_str) or {}

    for deprecated_field, _ in dict_deprecated_fields_with_defaults.items():
        if deprecated_field in yml_dict:
            if not exclude_dbt_project_keys:
                # by default we remove it
                refactored = True
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Removed the deprecated field '{deprecated_field}'",
                        deprecation=dict_fields_to_deprecation_class[deprecated_field]
                    )
                )
                del yml_dict[deprecated_field]
            # with the special field, we only remove it if it's different from the default
            elif yml_dict[deprecated_field] != dict_deprecated_fields_with_defaults[deprecated_field]:
                refactored = True
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log= f"Removed the deprecated field '{deprecated_field}' that wasn't set to the default value",
                        deprecation=dict_fields_to_deprecation_class[deprecated_field]
                    )
                )
                del yml_dict[deprecated_field]

    # TODO: add tests for this
    for deprecated_field, new_field in dict_renamed_fields.items():
        if deprecated_field in yml_dict:
            refactored = True
            if new_field not in yml_dict:
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Renamed the deprecated field '{deprecated_field}' to '{new_field}'",
                        deprecation=dict_fields_to_deprecation_class[deprecated_field]
                    )
                )
                yml_dict[new_field] = yml_dict[deprecated_field]
            else:
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Added the config of the deprecated field '{deprecated_field}' to '{new_field}'",
                        deprecation=dict_fields_to_deprecation_class[deprecated_field]
                    )
                )
                yml_dict[new_field] = yml_dict[new_field] + yml_dict[deprecated_field]
            del yml_dict[deprecated_field]

    return YMLRuleRefactorResult(
        rule_name="remove_deprecated_config",
        refactored=refactored,
        refactored_yaml=DbtYAML().dump_to_string(yml_dict) if refactored else yml_str,  # type: ignore
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors
    )

def rec_check_yaml_path(
    yml_dict: Dict[str, Any],
    path: Path,
    node_fields: DbtProjectSpecs,
    refactor_logs: Optional[List[str]] = None,
):
    # we can't set refactor_logs as an empty list

    # TODO: what about individual models in the config there?
    # indivdual models would show up here but without the `.sql` (or `.py`)

    if not path.exists() and not _path_exists_as_file(path):
        return yml_dict, [] if refactor_logs is None else refactor_logs

    yml_dict_copy = yml_dict.copy() if yml_dict else {}
    for k, v in yml_dict_copy.items():
        log_msg = None
        if not (path / k).exists() and not _path_exists_as_file(path / k):
            # Built-in config missing "+"
            if k in node_fields.allowed_config_fields_dbt_project:
                new_k = f"+{k}"
                yml_dict[new_k] = v
                log_msg = f"Added '+' in front of the nested config '{k}'"
            # Custom config not in meta
            elif not k.startswith("+"):
                log_msg = f"Moved custom config '{k}' to '+meta'"
                meta = yml_dict.get("+meta", {})
                meta.update({k: v})
                yml_dict["+meta"] = meta
            
            if log_msg:
                if refactor_logs is None:
                    refactor_logs = [log_msg]
                else:
                    refactor_logs.append(log_msg)
            
                del yml_dict[k]
        elif isinstance(yml_dict[k], dict):
            new_dict, refactor_logs = rec_check_yaml_path(yml_dict[k], path / k, node_fields, refactor_logs)
            yml_dict[k] = new_dict
    return yml_dict, [] if refactor_logs is None else refactor_logs

def _path_exists_as_file(path: Path) -> bool:
    return path.with_suffix(".py").exists() or path.with_suffix(".sql").exists() or path.with_suffix(".csv").exists()

def changeset_dbt_project_prefix_plus_for_config(
    yml_str: str, path: Path, schema_specs: SchemaSpecs
) -> YMLRuleRefactorResult:
    """Update keys for the config in dbt_project.yml under to prefix it with a `+`"""
    all_refactor_logs: List[str] = []

    yml_dict = DbtYAML().load(yml_str) or {}

    for node_type, node_fields in schema_specs.dbtproject_specs_per_node_type.items():
        for k, v in (yml_dict.get(node_type) or {}).copy().items():
            # check if this is the project name
            if k == yml_dict["name"]:
                new_dict, refactor_logs = rec_check_yaml_path(v, path / node_type, node_fields)
                yml_dict[node_type][k] = new_dict
                all_refactor_logs.extend(refactor_logs)

            # top level config
            elif k in node_fields.allowed_config_fields_dbt_project:
                all_refactor_logs.append(f"Added '+' in front of top level config '{k}'")
                new_k = f"+{k}"
                yml_dict[node_type][new_k] = v
                del yml_dict[node_type][k]

            # otherwise, treat it as a package
            # TODO: if this is not valid, we could delete it as well
            else:
                packages_path = path / Path(yml_dict.get("packages-paths", "dbt_packages"))
                new_dict, refactor_logs = rec_check_yaml_path(v, packages_path / k / node_type, node_fields)
                yml_dict[node_type][k] = new_dict
                all_refactor_logs.extend(refactor_logs)

    refactored = len(all_refactor_logs) > 0
    deprecation_refactors = [
        DbtDeprecationRefactor(
            log=log,
            deprecation="MissingPlusPrefixDeprecation"
        )
        for log in all_refactor_logs
    ]
    return YMLRuleRefactorResult(
        rule_name="prefix_plus_for_config",
        refactored=refactored,
        refactored_yaml=DbtYAML().dump_to_string(yml_dict) if refactored else yml_str,  # type: ignore
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors,
    )


def changeset_dbt_project_flip_behavior_flags(yml_str: str) -> YMLRuleRefactorResult:
    yml_dict = DbtYAML().load(yml_str) or {}
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    refactored = False

    behavior_change_flag_to_explainations = {
        "source_freshness_run_project_hooks": "run project hooks (on-run-start/on-run-end) as part of source freshness commands"
    }

    for key in yml_dict:
        if key == "flags":
            for behavior_change_flag in behavior_change_flag_to_explainations:
                if yml_dict["flags"].get(behavior_change_flag) is False:
                    yml_dict["flags"][behavior_change_flag] = True
                    refactored = True
                    deprecation_refactors.append(
                        DbtDeprecationRefactor(
                            log=f"Set flag '{behavior_change_flag}' to 'True' - This will {behavior_change_flag_to_explainations[behavior_change_flag]}.",
                            deprecation="SourceFreshnessProjectHooksNotRun"
                        )
                    )

    return YMLRuleRefactorResult(
            rule_name="flip_behavior_flags",
            refactored=refactored,
            refactored_yaml=DbtYAML().dump_to_string(yml_dict) if refactored else yml_str,  # type: ignore
            original_yaml=yml_str,
            deprecation_refactors=deprecation_refactors,
        )


def changeset_dbt_project_flip_test_arguments_behavior_flag(yml_str: str) -> YMLRuleRefactorResult:
    yml_dict = DbtYAML().load(yml_str) or {}
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    refactored = False

    existing_flags = yml_dict.get("flags", {})
    if existing_flags.get("require_generic_test_arguments_property") is False or "require_generic_test_arguments_property" not in existing_flags:
        yml_dict["flags"] = existing_flags
        yml_dict["flags"]["require_generic_test_arguments_property"] = True
        refactored = True
        deprecation_refactors.append(
            DbtDeprecationRefactor(
                log="Set flag 'require_generic_test_arguments_property' to 'True' - This will parse the values defined within the `arguments` property of test definition as the test keyword arguments.",
                deprecation="MissingGenericTestArgumentsPropertyDeprecation"
            )
        )

    return YMLRuleRefactorResult(
            rule_name="changeset_dbt_project_flip_test_arguments_behavior_flag",
            refactored=refactored,
            refactored_yaml=DbtYAML().dump_to_string(yml_dict) if refactored else yml_str,  # type: ignore
            original_yaml=yml_str,
            deprecation_refactors=deprecation_refactors,
        )


def changeset_fix_space_after_plus(yml_str: str, schema_specs: SchemaSpecs) -> YMLRuleRefactorResult:
    """Fix keys that have a space after the '+' prefix (e.g., '+ tags' -> '+tags').
    
    This fixes the dbt1060 error: "Ignored unexpected key '+ tags'".
    When users accidentally add a space after the '+' in config keys, it creates
    an invalid key. This function detects and fixes such keys, but only if the
    corrected key is valid according to the schema.
    
    Args:
        yml_str: The YAML string to process
        schema_specs: The schema specifications to validate corrected keys against
        
    Returns:
        YMLRuleRefactorResult containing the refactored YAML and any changes made
    """
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    
    # Pattern to match keys with space after plus: "+ key:" at the start of the line (after indentation)
    # We need to be careful to only match actual keys, not values
    pattern = re.compile(r'^(\s*)\+\s+(\w+)(\s*:)', re.MULTILINE)
    
    # First, let's identify all the matches and validate them
    matches = list(pattern.finditer(yml_str))
    
    if not matches:
        return YMLRuleRefactorResult(
            rule_name="fix_space_after_plus",
            refactored=False,
            refactored_yaml=yml_str,
            original_yaml=yml_str,
            deprecation_refactors=[],
        )
    
    # Collect all valid config keys from schema specs (with + prefix)
    all_valid_config_keys = set()
    for node_type, node_fields in schema_specs.dbtproject_specs_per_node_type.items():
        all_valid_config_keys.update(node_fields.allowed_config_fields_dbt_project_with_plus)
    
    # Build the refactored string by replacing matches
    refactored_yaml = yml_str
    offset = 0  # Track offset due to string length changes
    
    for match in matches:
        indent = match.group(1)
        key_name = match.group(2)
        colon_and_space = match.group(3)
        
        # The corrected key would be "+key"
        corrected_key = f"+{key_name}"
        
        # Only fix if the corrected key is valid according to schema
        if corrected_key not in all_valid_config_keys:
            # Skip this match - the corrected key is not valid
            continue
        
        original_full_match = match.group(0)
        corrected_full = f"{indent}{corrected_key}{colon_and_space}"
        
        # Calculate positions with offset
        start_pos = match.start() + offset
        end_pos = match.end() + offset
        
        # Replace in the string
        refactored_yaml = refactored_yaml[:start_pos] + corrected_full + refactored_yaml[end_pos:]
        
        # Update offset
        offset += len(corrected_full) - len(original_full_match)
        
        # Calculate line number for logging
        line_num = yml_str[:match.start()].count('\n') + 1
        
        refactored = True
        deprecation_refactors.append(
            DbtDeprecationRefactor(
                log=f"Removed space after '+' in key '+ {key_name}' on line {line_num}, changed to '{corrected_key}'"
            )
        )
    
    return YMLRuleRefactorResult(
        rule_name="fix_space_after_plus",
        refactored=refactored,
        refactored_yaml=refactored_yaml,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors,
    )