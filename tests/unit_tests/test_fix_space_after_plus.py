"""Tests for changeset_fix_space_after_plus function"""

import pytest
from yaml import safe_load

from dbt_autofix.refactors.changesets.dbt_project_yml import changeset_fix_space_after_plus


class MockSchemaSpecs:
    """Mock SchemaSpecs for testing"""
    def __init__(self):
        self.dbtproject_specs_per_node_type = {}


@pytest.fixture
def schema_specs():
    """Fixture to provide mock schema specs for tests"""
    return MockSchemaSpecs()


class TestFixSpaceAfterPlus:
    """Tests for fixing space after plus in config keys"""

    def test_no_space_after_plus_no_changes(self, schema_specs: MockSchemaSpecs):
        """Test that YAML without space after plus is not modified"""
        input_yaml = """
name: my_project
version: 1.0

models:
  my_project:
    +materialized: table
    +tags:
      - my-tag
"""
        result = changeset_fix_space_after_plus(input_yaml, schema_specs)
        assert not result.refactored
        assert len(result.deprecation_refactors) == 0
        assert result.refactored_yaml == input_yaml

    def test_single_space_after_plus(self, schema_specs: MockSchemaSpecs):
        """Test that single space after plus is fixed"""
        input_yaml = """
name: my_project
version: 1.0

models:
  my_project:
    + tags:
      - my-tag
"""
        expected_yaml = """
name: my_project
version: 1.0

models:
  my_project:
    +tags:
      - my-tag
"""
        result = changeset_fix_space_after_plus(input_yaml, schema_specs)
        assert result.refactored
        assert len(result.deprecation_refactors) == 1
        assert "Removed space after '+' in key '+ tags'" in result.deprecation_refactors[0].log
        assert result.refactored_yaml == expected_yaml

    def test_multiple_spaces_after_plus(self, schema_specs: MockSchemaSpecs):
        """Test that multiple instances of space after plus are all fixed"""
        input_yaml = """
name: my_project
version: 1.0

models:
  my_project:
    + tags:
      - my-tag
    + materialized: table
    + schema: my_schema
"""
        result = changeset_fix_space_after_plus(input_yaml, schema_specs)
        assert result.refactored
        assert len(result.deprecation_refactors) == 3
        
        # Verify all three keys were fixed
        assert "+tags:" in result.refactored_yaml
        assert "+materialized:" in result.refactored_yaml
        assert "+schema:" in result.refactored_yaml
        
        # Verify no spaces remain after plus
        assert "+ tags:" not in result.refactored_yaml
        assert "+ materialized:" not in result.refactored_yaml
        assert "+ schema:" not in result.refactored_yaml

    def test_mixed_correct_and_incorrect_keys(self, schema_specs: MockSchemaSpecs):
        """Test that only incorrect keys are fixed"""
        input_yaml = """
name: my_project
version: 1.0

models:
  my_project:
    +materialized: table
    + tags:
      - my-tag
    +schema: my_schema
"""
        result = changeset_fix_space_after_plus(input_yaml, schema_specs)
        assert result.refactored
        assert len(result.deprecation_refactors) == 1
        
        # Verify the incorrect key was fixed
        assert "+tags:" in result.refactored_yaml
        assert "+ tags:" not in result.refactored_yaml
        
        # Verify correct keys remain unchanged
        assert "+materialized: table" in result.refactored_yaml
        assert "+schema: my_schema" in result.refactored_yaml

    def test_nested_config_keys(self, schema_specs: MockSchemaSpecs):
        """Test that space after plus is fixed in nested structures"""
        input_yaml = """
name: my_project
version: 1.0

models:
  my_project:
    folder1:
      + tags:
        - tag1
      subfolder:
        + materialized: view
"""
        result = changeset_fix_space_after_plus(input_yaml, schema_specs)
        assert result.refactored
        assert len(result.deprecation_refactors) == 2
        
        # Verify both nested keys were fixed
        assert "+tags:" in result.refactored_yaml
        assert "+materialized:" in result.refactored_yaml
        assert "+ tags:" not in result.refactored_yaml
        assert "+ materialized:" not in result.refactored_yaml

    def test_preserves_yaml_structure(self, schema_specs: MockSchemaSpecs):
        """Test that YAML structure and other content is preserved"""
        input_yaml = """
name: my_project
version: 1.0

# This is a comment
models:
  my_project:
    + tags:  # inline comment
      - my-tag
    description: "Some description"
"""
        result = changeset_fix_space_after_plus(input_yaml, schema_specs)
        assert result.refactored
        
        # Verify comments are preserved
        assert "# This is a comment" in result.refactored_yaml
        assert "# inline comment" in result.refactored_yaml
        
        # Verify other keys are preserved
        assert 'description: "Some description"' in result.refactored_yaml
        
        # Verify the fix was applied
        assert "+tags:" in result.refactored_yaml

    def test_different_indentation_levels(self, schema_specs: MockSchemaSpecs):
        """Test that space after plus is fixed at different indentation levels"""
        input_yaml = """
name: my_project

models:
  + tags:
    - top-level
  my_project:
    + tags:
      - project-level
    folder:
      + tags:
        - folder-level
"""
        result = changeset_fix_space_after_plus(input_yaml, schema_specs)
        assert result.refactored
        assert len(result.deprecation_refactors) == 3
        
        # All should be fixed
        refactored_lines = result.refactored_yaml.split('\n')
        plus_tag_lines = [line for line in refactored_lines if 'tags:' in line and '+' in line]
        
        # All should have '+tags:' without space
        for line in plus_tag_lines:
            assert '+tags:' in line
            assert '+ tags:' not in line

    def test_empty_yaml(self, schema_specs: MockSchemaSpecs):
        """Test that empty YAML is handled correctly"""
        input_yaml = ""
        result = changeset_fix_space_after_plus(input_yaml, schema_specs)
        assert not result.refactored
        assert len(result.deprecation_refactors) == 0

    def test_yaml_with_no_plus_keys(self, schema_specs: MockSchemaSpecs):
        """Test that YAML without any plus-prefixed keys is not modified"""
        input_yaml = """
name: my_project
version: 1.0

models:
  my_project:
    materialized: table
    tags:
      - my-tag
"""
        result = changeset_fix_space_after_plus(input_yaml, schema_specs)
        assert not result.refactored
        assert len(result.deprecation_refactors) == 0
        assert result.refactored_yaml == input_yaml

    def test_line_number_in_log(self, schema_specs: MockSchemaSpecs):
        """Test that the log message includes the correct line number"""
        input_yaml = """name: my_project
version: 1.0

models:
  my_project:
    + tags:
      - my-tag
"""
        result = changeset_fix_space_after_plus(input_yaml, schema_specs)
        assert result.refactored
        assert len(result.deprecation_refactors) == 1
        # The '+ tags:' is on line 6
        assert "line 6" in result.deprecation_refactors[0].log

    def test_multiple_word_keys_not_matched(self, schema_specs: MockSchemaSpecs):
        """Test that keys with multiple words (invalid pattern) are not matched"""
        input_yaml = """
name: my_project

models:
  my_project:
    + my tags:  # This is invalid and won't be matched by our regex
      - tag1
"""
        result = changeset_fix_space_after_plus(input_yaml, schema_specs)
        # Should not match because "my tags" has a space in the key name itself
        assert not result.refactored
        assert len(result.deprecation_refactors) == 0

    def test_seeds_section(self, schema_specs: MockSchemaSpecs):
        """Test that space after plus is fixed in seeds section"""
        input_yaml = """
name: my_project

seeds:
  my_project:
    + schema: staging
    + tags:
      - seed-tag
"""
        result = changeset_fix_space_after_plus(input_yaml, schema_specs)
        assert result.refactored
        assert len(result.deprecation_refactors) == 2
        assert "+schema:" in result.refactored_yaml
        assert "+tags:" in result.refactored_yaml

    def test_tests_section(self, schema_specs: MockSchemaSpecs):
        """Test that space after plus is fixed in tests section"""
        input_yaml = """
name: my_project

tests:
  my_project:
    + schema: tests
    + store_failures: true
"""
        result = changeset_fix_space_after_plus(input_yaml, schema_specs)
        assert result.refactored
        assert len(result.deprecation_refactors) == 2
        assert "+schema:" in result.refactored_yaml
        assert "+store_failures:" in result.refactored_yaml

    def test_snapshots_section(self, schema_specs: MockSchemaSpecs):
        """Test that space after plus is fixed in snapshots section"""
        input_yaml = """
name: my_project

snapshots:
  my_project:
    + target_schema: snapshots
    + tags:
      - snapshot-tag
"""
        result = changeset_fix_space_after_plus(input_yaml, schema_specs)
        assert result.refactored
        assert len(result.deprecation_refactors) == 2
        assert "+target_schema:" in result.refactored_yaml
        assert "+tags:" in result.refactored_yaml

    def test_real_world_example_from_issue(self, schema_specs: MockSchemaSpecs):
        """Test the real-world example from the GitHub issue"""
        input_yaml = """
name: my_project

models:
  et_landed_consignments:
    +tags:
      - et_landed_consignments
  et_lapsed_consignors:
    + tags:
      - et_lapsed_consignors
  et_lapsed_credit_automation:
    +tags:
      - et_lapsed_credit_automation
"""
        result = changeset_fix_space_after_plus(input_yaml, schema_specs)
        assert result.refactored
        assert len(result.deprecation_refactors) == 1
        
        # Verify the problematic key was fixed
        assert "+ tags:" not in result.refactored_yaml
        
        # Verify all +tags are now correct
        lines_with_tags = [line for line in result.refactored_yaml.split('\n') if 'tags:' in line]
        for line in lines_with_tags:
            if '+' in line:
                assert '+tags:' in line
                assert '+ tags:' not in line
