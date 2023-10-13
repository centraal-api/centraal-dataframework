"""Prueba gx."""
from centraal_dataframework.tasks import task_dq


@task_dq
def hello_gx(datalake, datasource, logger):
    """Saluda al gx."""
    source = datalake.read_csv("test/test_framework.csv", sep="|")
    logger.info(source.head(1))
    logger.info("hello there, iam a task")


@task_dq(source_name="logica_negocio")
def hello_gx_negocio(datalake, datasource, logger):
    """Saluda al gx con logica."""
    source = datalake.read_csv("test/test_framework.csv", sep="|")
    logger.info(source.head(1))
    logger.info("hello there, iam a task")
