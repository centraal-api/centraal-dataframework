"""Modulo de excepciones."""


class ErrorEnTarea(Exception):
    """Excepción para levantar cuando se presenta un error no controlado.

    Attributes:
        nombre de la tarea que fallo.
    """

    def __init__(self, task_name: str):
        """Incializa con el nombre de la tarea."""
        self.message = f"Se encontro un error en {task_name}."
        super().__init__(self.message)
