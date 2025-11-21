"""
Simple input/output tests for rec_check_yaml_path function.

Each test shows:
- Input: what goes into the function
- Expected Output: what we expect to get back
- Why: explanation of the transformation

🎯 KEY FEATURE: These tests use the REAL dbt Fusion schema, not mocks!

This means:
✅ Tests are always accurate to the actual Fusion schema
✅ Tests will catch if Fusion schema changes
✅ No need to keep mocks in sync with reality
✅ You can trust that these tests reflect real-world behavior

The schema is fetched once per test run (module scope fixture) for efficiency.

🔧 LOGIC: Simple validation - no spelling correction!
- If config is IN schema → keep it
- If config is NOT in schema → move to +meta
- No attempts to "fix" typos or rename configs
"""

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from dbt_autofix.refactors.changesets.dbt_project_yml import rec_check_yaml_path
from dbt_autofix.retrieve_schemas import SchemaSpecs


@pytest.fixture(scope="module")
def real_schema():
    """
    Provides REAL dbt Fusion schema specs.
    This fetches the actual schema from dbt Fusion, so tests are accurate!
    """
    return SchemaSpecs()


@pytest.fixture
def models_node_fields(real_schema):
    """
    Provides the real node fields for models from dbt Fusion schema.
    This tells us what config keys are actually valid for models in dbt_project.yml.
    """
    return real_schema.dbtproject_specs_per_node_type['models']


@pytest.fixture
def temp_path():
    """Provides a temp directory for path validation"""
    with TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


# =============================================================================
# SCENARIO 1: Correct syntax - no changes needed
# =============================================================================

def test_correct_hook_syntax_unchanged(models_node_fields, temp_path):
    """
    INPUT:  +post-hook with hyphen (correct for dbt_project.yml)
    OUTPUT: Same, no changes
    WHY:    This is the correct syntax according to the REAL Fusion schema
    """
    input_dict = {
        '+post-hook': ['select 1', 'select 2'],
        '+pre-hook': 'select 0'
    }
    
    expected_output = {
        '+post-hook': ['select 1', 'select 2'],
        '+pre-hook': 'select 0'
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 0  # No changes = no logs


def test_correct_configs_unchanged(models_node_fields, temp_path):
    """
    INPUT:  Valid configs with + prefix
    OUTPUT: Same, no changes
    WHY:    All configs are recognized and properly formatted
    """
    input_dict = {
        '+materialized': 'table',
        '+tags': ['tag1', 'tag2'],
        '+enabled': True
    }
    
    expected_output = {
        '+materialized': 'table',
        '+tags': ['tag1', 'tag2'],
        '+enabled': True
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 0


# =============================================================================
# SCENARIO 2: Schema does not support the key - moved to meta
# =============================================================================

def test_unsupported_hook_moved_to_meta(models_node_fields, temp_path):
    """
    INPUT:  +post_hook with underscore (NOT in schema)
    OUTPUT: Moved to +meta.post_hook
    WHY:    Schema doesn't recognize post_hook, so it's treated as custom config
    """
    input_dict = {
        '+post_hook': 'grant select',
        '+pre_hook': 'begin'
    }
    
    expected_output = {
        '+meta': {
            'post_hook': 'grant select',  # Moved to meta
            'pre_hook': 'begin'           # Moved to meta
        }
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 2
    assert 'post_hook' in logs[0] and 'meta' in logs[0]
    assert 'pre_hook' in logs[1] and 'meta' in logs[1]


def test_mixed_valid_and_invalid_configs(models_node_fields, temp_path):
    """
    INPUT:  Mix of valid (+post-hook) and invalid (+post_hook) configs
    OUTPUT: Valid kept, invalid moved to meta
    WHY:    Only schema-supported configs stay at top level
    """
    input_dict = {
        '+post-hook': 'select 1',  # Valid, keep
        '+post_hook': 'select 2',  # Invalid (underscore), move to meta
        '+materialized': 'table'   # Valid, keep
    }
    
    expected_output = {
        '+post-hook': 'select 1',
        '+materialized': 'table',
        '+meta': {
            'post_hook': 'select 2'  # Moved
        }
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 1  # Only one move needed
    assert 'post_hook' in logs[0] and 'meta' in logs[0]


# =============================================================================
# SCENARIO 3: Missing + prefix
# =============================================================================

def test_valid_config_missing_plus(models_node_fields, temp_path):
    """
    INPUT:  materialized without + prefix
    OUTPUT: +materialized with + prefix added
    WHY:    In dbt_project.yml, config keys need + prefix
    """
    input_dict = {
        'materialized': 'table',
        'tags': ['tag1']
    }
    
    expected_output = {
        '+materialized': 'table',
        '+tags': ['tag1']
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 2
    assert any('materialized' in log for log in logs)
    assert any('tags' in log for log in logs)


def test_hook_missing_plus(models_node_fields, temp_path):
    """
    INPUT:  post-hook without + prefix (hyphen is correct, just missing +)
    OUTPUT: +post-hook with + prefix added
    WHY:    Config keys in dbt_project.yml need + prefix
    """
    input_dict = {
        'post-hook': 'select 1'
    }
    
    expected_output = {
        '+post-hook': 'select 1'
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 1
    assert 'post-hook' in logs[0]


# =============================================================================
# SCENARIO 4: Unknown/custom configs
# =============================================================================

def test_unknown_config_with_plus_moved_to_meta(models_node_fields, temp_path):
    """
    INPUT:  +custom_config (not in schema)
    OUTPUT: Moved to +meta.custom_config
    WHY:    Unknown configs must go under meta to avoid errors
    """
    input_dict = {
        '+custom_config': 'my_value',
        '+materialized': 'table'
    }
    
    expected_output = {
        '+materialized': 'table',
        '+meta': {
            'custom_config': 'my_value'
        }
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 1
    assert 'custom_config' in logs[0]
    assert 'meta' in logs[0]


def test_unknown_config_without_plus_moved_to_meta(models_node_fields, temp_path):
    """
    INPUT:  custom_field without + (not in schema)
    OUTPUT: Moved to +meta.custom_field
    WHY:    Unknown configs go to meta, even if they don't have +
    """
    input_dict = {
        'custom_field': 'value',
        'materialized': 'table'
    }
    
    expected_output = {
        '+materialized': 'table',
        '+meta': {
            'custom_field': 'value'
        }
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 2  # One for adding +, one for moving to meta


def test_multiple_unknowns_merged_into_meta(models_node_fields, temp_path):
    """
    INPUT:  Multiple unknown configs
    OUTPUT: All merged into single +meta block
    WHY:    Keep all custom configs organized under meta
    """
    input_dict = {
        '+unknown1': 'val1',
        '+unknown2': 'val2',
        '+unknown3': 'val3',
        '+materialized': 'table'
    }
    
    expected_output = {
        '+materialized': 'table',
        '+meta': {
            'unknown1': 'val1',
            'unknown2': 'val2',
            'unknown3': 'val3'
        }
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 3
    assert all('meta' in log for log in logs)


# =============================================================================
# SCENARIO 5: Complex real-world examples
# =============================================================================

def test_real_world_example_user_config(models_node_fields, temp_path):
    """
    INPUT:  Real user config with +post_hook (underscore - not in schema!)
    OUTPUT: Moved to +meta.post_hook
    WHY:    post_hook is not in schema, so treated as custom config
    
    NOTE: Using REAL schema - post_hook with underscore is NOT recognized
    """
    input_dict = {
        '+my_custom_unknown_config': 'value',
        '+post_hook': [
            "{{ grant_select(this, 'data-analytics') }}",
            "{{ grant_select(this, 'airflow_service_user_role') }}"
        ],
        '+materialized': 'table'
    }
    
    expected_output = {
        '+materialized': 'table',
        '+meta': {
            'my_custom_unknown_config': 'value',
            'post_hook': [  # Moved to meta (not in schema)
                "{{ grant_select(this, 'data-analytics') }}",
                "{{ grant_select(this, 'airflow_service_user_role') }}"
            ]
        }
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 2
    # Both moved to meta
    assert any('post_hook' in log and 'meta' in log for log in logs)
    assert any('my_custom_unknown_config' in log and 'meta' in log for log in logs)


def test_all_scenarios_combined(models_node_fields, temp_path):
    """
    INPUT:  Mix of all scenarios
    OUTPUT: Everything properly handled
    WHY:    Real projects have all these issues at once
    """
    input_dict = {
        '+post-hook': 'select 1',     # Valid, keep
        '+pre_hook': 'select 0',      # Invalid (underscore), move to meta
        'materialized': 'table',      # Missing +, add it
        '+enabled': True,             # Valid, keep
        '+custom_unknown': 'val',     # Invalid, move to meta
        'another_custom': 'val2'      # Invalid without +, move to meta
    }
    
    expected_output = {
        '+post-hook': 'select 1',
        '+materialized': 'table',     # + added!
        '+enabled': True,
        '+meta': {                    # All invalids here
            'pre_hook': 'select 0',
            'custom_unknown': 'val',
            'another_custom': 'val2'
        }
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 4
    # Should have logs for: materialized +, and 3 meta moves


# =============================================================================
# SCENARIO 6: Edge cases
# =============================================================================

def test_empty_dict(models_node_fields, temp_path):
    """
    INPUT:  Empty dict
    OUTPUT: Empty dict
    WHY:    Nothing to process
    """
    input_dict = {}
    expected_output = {}
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 0


def test_preserves_complex_values(models_node_fields, temp_path):
    """
    INPUT:  Configs with complex values (lists, dicts)
    OUTPUT: Values preserved exactly
    WHY:    We only change keys, never values
    """
    input_dict = {
        '+post-hook': [
            "{{ macro1() }}",
            "select * from {{ ref('model') }}"
        ],
        '+persist_docs': {
            'relation': True,
            'columns': True
        },
        '+tags': ['tag1', 'tag2', 'tag3']
    }
    
    expected_output = input_dict.copy()  # Should be unchanged
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 0


def test_jinja_in_hook_values_preserved(models_node_fields, temp_path):
    """
    INPUT:  Invalid config (+post_hook) with complex Jinja
    OUTPUT: Moved to meta with Jinja preserved exactly
    WHY:    We only move keys, never touch the values/Jinja content
    """
    input_dict = {
        '+post_hook': [
            "{%- if target.name.startswith('sf')-%}{{ grant_select(this, 'role') }}{%- endif -%}",
            "CALL UTILS.GOVERNANCE.REAPPLY_JIRA_RESOURCE_GRANTS('{{ this | upper }}')"
        ]
    }
    
    expected_output = {
        '+meta': {
            'post_hook': [  # Moved to meta, values preserved exactly
                "{%- if target.name.startswith('sf')-%}{{ grant_select(this, 'role') }}{%- endif -%}",
                "CALL UTILS.GOVERNANCE.REAPPLY_JIRA_RESOURCE_GRANTS('{{ this | upper }}')"
            ]
        }
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 1
    assert 'post_hook' in logs[0] and 'meta' in logs[0]


# =============================================================================
# SCENARIO 7: Nested configs under logical groupings (non-existent directories)
# =============================================================================

def test_nested_config_under_logical_grouping(models_node_fields, temp_path):
    """
    INPUT:  Logical grouping 'example' (doesn't exist as directory) with nested configs
    OUTPUT: Grouping preserved, nested configs get + prefix
    WHY:    Logical groupings in YAML should be recursed into, not moved to meta
    
    This is the bug from the user's question - materialized wasn't getting +
    """
    input_dict = {
        'example': {
            'materialized': 'view'
        }
    }
    
    expected_output = {
        'example': {
            '+materialized': 'view'
        }
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 1
    assert 'materialized' in logs[0]


def test_multiple_nested_configs_under_logical_grouping(models_node_fields, temp_path):
    """
    INPUT:  Logical grouping with multiple nested configs
    OUTPUT: All nested configs get + prefix
    WHY:    All valid configs in a logical grouping need + prefix
    """
    input_dict = {
        'example': {
            'materialized': 'view',
            'schema': 'analytics',
            'enabled': True
        }
    }
    
    expected_output = {
        'example': {
            '+materialized': 'view',
            '+schema': 'analytics',
            '+enabled': True
        }
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 3


def test_deeply_nested_logical_groupings(models_node_fields, temp_path):
    """
    INPUT:  Multiple levels of logical groupings
    OUTPUT: Configs at all levels get + prefix
    WHY:    Recursion should work at any depth
    """
    input_dict = {
        'external_views': {
            'example': {
                'materialized': 'view',
                'schema': 'analytics'
            }
        }
    }
    
    expected_output = {
        'external_views': {
            'example': {
                '+materialized': 'view',
                '+schema': 'analytics'
            }
        }
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 2


def test_users_exact_scenario_from_question(models_node_fields, temp_path):
    """
    INPUT:  The exact structure from the user's question
    OUTPUT: materialized gets + prefix
    WHY:    This is the bug that was reported
    
    User's YAML:
    models:
      external_views:
        example:
          materialized: view
        schema: external_analytics
    """
    input_dict = {
        'external_views': {
            'example': {
                'materialized': 'view'
            },
            'schema': 'external_analytics'
        }
    }
    
    expected_output = {
        'external_views': {
            'example': {
                '+materialized': 'view'
            },
            '+schema': 'external_analytics'
        }
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 2


def test_nested_custom_configs_in_logical_grouping(models_node_fields, temp_path):
    """
    INPUT:  Logical grouping with custom (invalid) configs
    OUTPUT: Custom configs moved to +meta at the right level
    WHY:    Custom configs should still move to meta, even in nested groupings
    """
    input_dict = {
        'example': {
            'materialized': 'view',
            'custom_unknown': 'value'
        }
    }
    
    expected_output = {
        'example': {
            '+materialized': 'view',
            '+meta': {
                'custom_unknown': 'value'
            }
        }
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 2  # One for materialized +, one for moving custom to meta


def test_mixed_logical_groupings_and_configs(models_node_fields, temp_path):
    """
    INPUT:  Top-level configs mixed with logical groupings
    OUTPUT: Both handled correctly
    WHY:    Real projects have configs at multiple levels
    """
    input_dict = {
        'materialized': 'table',  # Top-level config
        'example': {              # Logical grouping
            'materialized': 'view',
            'enabled': False
        }
    }
    
    expected_output = {
        '+materialized': 'table',
        'example': {
            '+materialized': 'view',
            '+enabled': False
        }
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 3


def test_logical_grouping_with_already_prefixed_configs(models_node_fields, temp_path):
    """
    INPUT:  Logical grouping where some configs already have +
    OUTPUT: Already prefixed configs unchanged, others get +
    WHY:    Partial migrations should work correctly
    """
    input_dict = {
        'example': {
            '+materialized': 'view',  # Already has +
            'schema': 'analytics'      # Missing +
        }
    }
    
    expected_output = {
        'example': {
            '+materialized': 'view',
            '+schema': 'analytics'
        }
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 1  # Only schema needs fixing


def test_empty_logical_grouping(models_node_fields, temp_path):
    """
    INPUT:  Logical grouping with empty dict
    OUTPUT: Empty dict preserved
    WHY:    Edge case - empty groupings should work
    """
    input_dict = {
        'example': {}
    }
    
    expected_output = {
        'example': {}
    }
    
    result, logs = rec_check_yaml_path(input_dict, temp_path, models_node_fields)
    
    assert result == expected_output
    assert len(logs) == 0


# =============================================================================
# Quick reference table (as docstring)
# =============================================================================

"""
QUICK REFERENCE - What rec_check_yaml_path does:

Input Key                  | Output Key           | Why
---------------------------|----------------------|----------------------------------
+post-hook                 | +post-hook          | ✅ In schema (hyphen) - kept
+post_hook                 | +meta.post_hook     | ❌ Not in schema - moved to meta
post-hook                  | +post-hook          | 🔧 Added + prefix (in schema)
+materialized              | +materialized       | ✅ In schema - kept
materialized               | +materialized       | 🔧 Added + prefix (in schema)
+file_format               | +file_format        | ✅ In schema - kept (real schema!)
+custom_config             | +meta.custom_config | ❌ Not in schema - moved to meta
custom_config              | +meta.custom_config | ❌ Not in schema - moved to meta

Key insights:
1. Only checks if key is IN SCHEMA - no spelling correction
2. dbt_project.yml schema expects: +pre-hook, +post-hook (hyphens)
3. All configs need + prefix in dbt_project.yml
4. Any config NOT in schema → moved to +meta
5. 🎯 Tests use REAL Fusion schema - accurate validation!
"""

