"""Modulo para test de tasks."""

import pytest
import pandas as pd
from great_expectations.data_context import EphemeralDataContext
from centraal_dataframework.tasks import GreatExpectationsToolKit


@pytest.fixture
def ge_toolkit() -> GreatExpectationsToolKit:
    """tollkit fixture."""
    return GreatExpectationsToolKit(EphemeralDataContext(**{"a": 123}), "test_task")


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """dataframe fixture."""
    data = {"column1": [1, 2, 3, 4, 5], "column2": ["A", "B", "C", "D", "E"]}
    return pd.DataFrame(data)


def test_run_expectations_on_df_should_return_valid_url(
    ge_toolkit: GreatExpectationsToolKit, sample_dataframe: pd.DataFrame
):
    """test para run_expectations_on_df."""
    expectations = [
        {"expectation_type": "expect_column_to_exist", "column": "column1"},
        {"expectation_type": "expect_column_to_exist", "column": "column2"},
    ]
    result = ge_toolkit.run_expectations_on_df(sample_dataframe, "test_df", expectations)

    assert isinstance(result, str)
    assert "z13.web" in result
