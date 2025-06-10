import pytest

from dbt_autofix.dbt_api import step_regex_replace


@pytest.mark.parametrize(
    "input_step,expected",
    [
        # Test -m to -s replacement
        ("dbt run -m model_name", "dbt run -s model_name"),
        ("dbt run -m model_name -t prod", "dbt run -s model_name -t prod"),
        ('dbt run -m model_name --vars \'{"key": "value"}\'', 'dbt run -s model_name --vars \'{"key": "value"}\''),
        # Test that -m is only replaced when it's a full word
        ("dbt run --models model_name", "dbt run --select model_name"),
        ("dbt run -mymodel", "dbt run -mymodel"),  # Should not replace
        ("dbt run -m", "dbt run -m"),  # Edge case: just -m
        # Test --model/--models to --select replacement
        ("dbt run --model model_name", "dbt run --select model_name"),
        ("dbt run --models model_name", "dbt run --select model_name"),
        ("dbt run --models model1 model2", "dbt run --select model1 model2"),
        # Test combinations
        ("dbt run -m model1 --models model2", "dbt run -s model1 --select model2"),
        # Test no changes needed
        ("dbt run -s model_name", "dbt run -s model_name"),
        ("dbt run --select model_name", "dbt run --select model_name"),
        # Test with other dbt commands
        ("dbt test -m model_name", "dbt test -s model_name"),
        ("dbt build -m model_name", "dbt build -s model_name"),
        # Test with multiple spaces
        ("dbt run  -m  model_name", "dbt run  -s  model_name"),
        ("dbt run  --models  model_name", "dbt run  --select  model_name"),
    ],
)
def test_step_regex_replace(input_step: str, expected: str):
    """Test the step_regex_replace function with various input scenarios."""
    assert step_regex_replace(input_step) == expected
