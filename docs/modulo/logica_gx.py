"""Prueba gx."""
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from centraal_dataframework.tasks import task_dq


@task_dq
def hello_gx(datalake, gx_toolkit, logger):
    """Saluda al gx."""
    source = datalake.read_csv("test/test_framework.csv", sep="|")
    logger.info(source.head(1))
    logger.info("hello there, iam a task_dq using great expectations")
    # creaciones de expectativas
    expectation_1 = ExpectationConfiguration(
        expectation_type="expect_table_columns_to_match_set",
        kwargs={
            'column_set': [
                'Producto',
                '2023-07-01',
                '2023-08-01',
                '2023-09-01',
                '2023-10-01',
                '2023-11-01',
                '2023-12-01',
                '2024-01-01',
                '2024-02-01',
                '2024-03-01',
                '2024-04-01',
                '2024-05-01',
                '2024-06-01',
            ]
        },
        meta={
            "notes": {
                "format": "markdown",
                "content": "Ejemplo de expectativa",
            }
        },
    )
    expectation_2 = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null", kwargs={"column": "Producto"}
    )

    expectation_3 = ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_be_between",
        kwargs={"max_value": 160, "min_value": 131},
        meta={
            "notes": {
                "format": "markdown",
                "content": "**Ejemplo de expectativa** que va fallar",
            }
        },
    )
    url = gx_toolkit.run_expectations_on_df(source, "test", [expectation_1, expectation_2, expectation_3])
    print("reporte de expectativas", url)
