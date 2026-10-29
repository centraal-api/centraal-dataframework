"""Framework para ejecutar transformaciones y calidad de datos en Azure Functions."""

__author__ = """centraal.studio"""
__email__ = 'equipo@centraal.studio'
__version__ = '0.2.0'

from centraal_dataframework.excepciones import (
    ErrorEnTarea,
    ErrorTareaCalidadDatos,
    RecursoDesconocido,
    RecursoNoConfigurado,
    RecursoNoDeclarado,
    TareaDuplicada,
    TareaNoDefinida,
)
from centraal_dataframework.registry import register_resource
from centraal_dataframework.resources import DQResult
from centraal_dataframework.synapse import SynapseClient
from centraal_dataframework.tasks import TaskContext, task

__all__ = [
    'task',
    'TaskContext',
    'register_resource',
    'DQResult',
    'SynapseClient',
    'ErrorEnTarea',
    'ErrorTareaCalidadDatos',
    'TareaDuplicada',
    'TareaNoDefinida',
    'RecursoDesconocido',
    'RecursoNoDeclarado',
    'RecursoNoConfigurado',
    '__version__',
]
