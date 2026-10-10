"""Ejemplo de tarea DQ con Great Expectations 1.x."""
import great_expectations as gx

from centraal_dataframework import TaskContext, task_dq


@task_dq
def hello_gx(ctx: TaskContext):
    """Valida un CSV con expectativas GX 1.x."""
    source = ctx.datalake.read_csv("test/test_framework.csv", sep="|")
    ctx.logger.info(source.head(1))

    expectations = [
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=[
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
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="Producto"),
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=131, max_value=160),
    ]
    result = ctx.gx.validate(source, "test", expectations)
    ctx.logger.info("reporte de expectativas %s success=%s", result.url, result.success)
    return result
