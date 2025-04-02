import tempfile
from pathlib import Path

import pytest
from yaml import safe_load

from dbt_cleanup.refactor import (
    RefactorResult,
    changeset_all_yml_files,
    changeset_refactor_yml,
    load_yaml_check_duplicates,
    output_yaml,
)


@pytest.fixture
def temp_project_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        project_dir = Path(tmpdirname)

        # Create dbt_project.yml
        project_dir.joinpath("dbt_project.yml").write_text("""
model-paths: ["models"]
""")

        # Create models directory
        models_dir = project_dir / "models"
        models_dir.mkdir(parents=True, exist_ok=True)

        yield project_dir


@pytest.fixture
def schema_yml_with_duplicates():
    return """
version: 2

models:
  - name: my_first_dbt_model
    description: "A starter dbt model"
    description: "A starter dbt model second"
    desciption: "A starter dbt model third"
    zorderr: 30
    meta:
      abc: 123
    config:
      meta:
        def: 456
    columns:
      - name: id
        description: "The primary key for this table"
        data_tests:
          - unique
          - not_null

  - name: my_second_dbt_model
    # this is a comment
    description: "A starter dbt model" # and a comment
    desciption: "A starter dbt model" # and a commenttt
    # this is a comment
    materialized: table # yep
    config:
      meta: 
       abc: 123
    columns:
      - name: id
        description: "The primary key for this table"
        data_tests:
          - unique
          - not_null
"""


@pytest.fixture
def schema_yml_with_config_fields():
    return """
version: 2

models:
  - name: my_first_dbt_model
    description: "A starter dbt model"
    materialized: table
    database: my_db
    schema: my_schema
    meta:
      abc: 123
    columns:
      - name: id
        description: "The primary key for this table"
        data_tests:
          - unique
          - not_null
"""


def test_changeset_refactor_yml_with_duplicates(temp_project_dir, schema_yml_with_duplicates):
    # Create a test YAML file
    yml_file = temp_project_dir / "models" / "schema.yml"
    yml_file.parent.mkdir(parents=True, exist_ok=True)
    yml_file.write_text(schema_yml_with_duplicates)

    # Test that loading a file with duplicates exits the program
    with pytest.raises(SystemExit):
        load_yaml_check_duplicates(yml_file)


def test_changeset_refactor_yml_with_config_fields(temp_project_dir, schema_yml_with_config_fields):
    # Create a test YAML file
    yml_file = temp_project_dir / "models" / "schema.yml"
    yml_file.parent.mkdir(parents=True, exist_ok=True)
    yml_file.write_text(schema_yml_with_config_fields)

    # Get the refactored result
    result = changeset_refactor_yml(yml_file)

    # Check that the file was refactored
    assert result.refactored
    assert isinstance(result, RefactorResult)

    # Check that config fields were moved under config
    model = safe_load(result.refactored_yaml)["models"][0]

    assert "materialized" not in model
    assert "database" not in model
    assert "schema" not in model
    assert "config" in model
    assert model["config"]["materialized"] == "table"
    assert model["config"]["database"] == "my_db"
    assert model["config"]["schema"] == "my_schema"

    # Check that meta was merged correctly
    assert model["config"]["meta"]["abc"] == 123


def test_changeset_all_yml_files(temp_project_dir, schema_yml_with_config_fields):
    # Create multiple YAML files
    models_dir = temp_project_dir / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    # Create a subdirectory with another YAML file
    sub_dir = models_dir / "example"
    sub_dir.mkdir(parents=True, exist_ok=True)
    sub_dir.joinpath("model.sql").write_text("not a YAML file")

    # Write YAML files
    models_dir.joinpath("schema.yml").write_text(schema_yml_with_config_fields)
    sub_dir.joinpath("other_schema.yaml").write_text(schema_yml_with_config_fields)

    # Get all refactored results
    results = changeset_all_yml_files(temp_project_dir)

    # Check that we got results for both files
    assert len(results) == 2
    assert all(isinstance(r, RefactorResult) for r in results)
    assert all(r.refactored for r in results)

    # Check that both files were processed
    processed_files = {r.file_path for r in results}
    assert (models_dir / "schema.yml").resolve() in processed_files
    assert (sub_dir / "other_schema.yaml").resolve() in processed_files


def test_output_yaml():
    # Test that output_yaml produces valid YAML
    test_data = {
        "version": 2,
        "models": [
            {
                "name": "test_model",
                "description": "Test model",
                "config": {"materialized": "table", "meta": {"abc": 123}},
            }
        ],
    }

    yaml_str = output_yaml(test_data)
    assert "version: 2" in yaml_str
    assert "name: test_model" in yaml_str
    assert "materialized: table" in yaml_str
    assert "abc: 123" in yaml_str
