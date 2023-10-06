"""Prueba."""

from centraal_dataframework.tasks import task


@task
def greetins():
    """Saluda."""
    print("hello there, iam a task")


@task
def hello():
    """Saluda."""
    print("hola, que mas pues!, soy una tarea")
