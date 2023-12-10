"""Prueba."""
from centraal_dataframework.tasks import task


@task
def hello_datalake(datalake, logger):
    """Saluda al datalake."""
    source = datalake.read_csv("test/test_framework.csv", sep="|")
    logger.info(source.head(1))
    logger.info("hello there, iam a task")
