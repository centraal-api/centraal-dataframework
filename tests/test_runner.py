"""Tests para el modulo runner."""
from datetime import datetime, timezone

import pytest

from centraal_dataframework.runner import Runner


@pytest.fixture(name="runner_for_test")
def runner_for_test_fixture() -> Runner:
    """Fixture para test."""
    return Runner()


@pytest.mark.parametrize(
    "task_config, execution_date, expected_result",
    [
        # pylint: disable=line-too-long
        ({"dias": "0,2,3", "horas": "3,12,16"}, datetime(2023, 1, 2, 8, 0, tzinfo=timezone.utc), True),
        ({"dias": "1,2,3", "horas": "8,12,16"}, datetime(2023, 1, 1, 18, 0, tzinfo=timezone.utc), False),
        ({"dias": "*", "horas": "*"}, None, True),
    ],
)
def test_is_scheduled_should_return_cuando_task_esprogramable(
    runner_for_test: Runner, task_config: dict, execution_date: datetime, expected_result: bool
):
    """Test para verificar el comportamiento de `es_programable`."""
    assert runner_for_test.es_programable(task_config, execution_date) == expected_result
