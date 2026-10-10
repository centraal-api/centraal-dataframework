"""Ejemplo de tarea de transformación (API 0.2)."""
from centraal_dataframework import TaskContext, task


@task
def hello_datalake(ctx: TaskContext):
    """Saluda al datalake."""
    source = ctx.datalake.read_csv("test/test_framework.csv", sep="|")
    ctx.logger.info(source.head(1))
    ctx.logger.info("hello there, i am a task. params=%s", ctx.params)
