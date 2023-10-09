"""Prueba."""

from centraal_dataframework.tasks import task


@task
def hello_datalake(datalake, logger):
    """Saluda."""
    source = datalake.read_csv("test/test_framework.csv", sep="|")
    logger.info(source.head(1))
    logger.info("hello there, iam a task")


@task
def hello_great_expectations(datalake, logger):
    """Saluda."""
    print("hola, que mas pues!, soy una tarea")
    source = datalake.read_csv("test/test_framework.csv", sep="|")
    logger.info(source.tail(1))
