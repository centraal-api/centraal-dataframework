"""Prueba."""

from centraal_dataframework.tasks import task


@task
def greetins():
    """Saluda."""
    print("hello there, iam a task")
