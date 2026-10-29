"""Registro de plataformas que una tarea puede cargar en su contexto.

Cada plataforma (datalake, synapse, gx, ...) se registra con un nombre, una factory que crea el cliente
y una verificación de configuración que no se conecta. Las del core usan la misma API que un proyecto
cliente usaría para registrar una propia.

Una tarea declara lo que usa con ``uses``: ``'synapse'`` es la conexión por defecto y ``'synapse:gold'``
una conexión con nombre, configurada con variables con sufijo (``SYNAPSE_WORKSPACE__GOLD``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional, Tuple

from centraal_dataframework.excepciones import RecursoDesconocido
from centraal_dataframework.settings import Settings, get_settings, settings_for_instance

if TYPE_CHECKING:  # pragma: no cover
    from centraal_dataframework.tasks import TaskContext

Factory = Callable[[Settings, 'TaskContext'], Any]
Check = Callable[[Settings], List[str]]

_NAME_RE = re.compile(r'[A-Za-z][A-Za-z0-9_]*')
_INSTANCE_RE = re.compile(r'[A-Za-z0-9_]+')


@dataclass(frozen=True)
class Resource:
    """Plataforma registrada."""

    name: str
    factory: Factory
    check: Optional[Check] = None


_REGISTRY: Dict[str, Resource] = {}


def register_resource(name: str, factory: Factory, *, check: Optional[Check] = None, replace: bool = False) -> None:
    """Registra una plataforma.

    Args:
        name: Nombre con el que las tareas la declaran en ``uses`` (letras, números y ``_``).
        factory: ``factory(settings, ctx)`` crea el cliente. Recibe las Settings de la conexión
            (por defecto o con nombre) y el TaskContext de la ejecución.
        check: ``check(settings)`` devuelve la lista de problemas de configuración sin conectarse;
            lista vacía si está completa.
        replace: Permite reemplazar una plataforma ya registrada (p.ej. en tests).
    """
    if not _NAME_RE.fullmatch(name):
        raise ValueError(f"Nombre de plataforma inválido: {name!r}")
    if name in _REGISTRY and not replace:
        raise ValueError(f"La plataforma {name!r} ya está registrada; use replace=True para reemplazarla.")
    _REGISTRY[name] = Resource(name=name, factory=factory, check=check)


def unregister_resource(name: str) -> None:
    """Quita una plataforma del registro (tests)."""
    _REGISTRY.pop(name, None)


def get_resource(name: str) -> Resource:
    """Obtiene una plataforma registrada."""
    try:
        return _REGISTRY[name]
    except KeyError:
        raise RecursoDesconocido(name, sorted(_REGISTRY)) from None


def registered_resources() -> List[str]:
    """Nombres de las plataformas registradas."""
    return sorted(_REGISTRY)


def parse_use(use: str) -> Tuple[str, Optional[str]]:
    """Separa ``'plataforma:instancia'`` en ``(plataforma, instancia)``; instancia None si no hay."""
    name, sep, instance = use.partition(':')
    if not _NAME_RE.fullmatch(name) or (sep and not _INSTANCE_RE.fullmatch(instance)):
        raise ValueError(f"Recurso inválido: {use!r}. Use 'plataforma' o 'plataforma:instancia'.")
    return name, (instance.lower() if sep else None)


def format_use(name: str, instance: Optional[str]) -> str:
    """Inverso de parse_use."""
    return f"{name}:{instance}" if instance else name


def normalize_uses(uses: Iterable[str]) -> Tuple[str, ...]:
    """Valida ``uses`` contra el registro y devuelve los usos normalizados sin duplicados."""
    if isinstance(uses, str):
        uses = [uses]
    normalized: List[str] = []
    for use in uses:
        name, instance = parse_use(use)
        get_resource(name)
        key = format_use(name, instance)
        if key not in normalized:
            normalized.append(key)
    return tuple(normalized)


def settings_for(instance: Optional[str], base: Optional[Settings] = None) -> Settings:
    """Settings de una conexión: base (o las globales) si instance es None, o la conexión con nombre."""
    if instance is None:
        return base or get_settings()
    return settings_for_instance(instance, base)


def check_uses(uses: Iterable[str], base: Optional[Settings] = None) -> List[str]:
    """Problemas de configuración de los recursos declarados (sin conectarse)."""
    problems: List[str] = []
    for use in uses:
        name, instance = parse_use(use)
        resource = get_resource(name)
        if resource.check is None:
            continue
        try:
            found = resource.check(settings_for(instance, base))
        except Exception as exc:  # noqa: BLE001
            found = [f"{type(exc).__name__}: {exc}"]
        problems.extend(f"{format_use(name, instance)}: {p}" for p in found)
    return problems


# --- Plataformas del core -------------------------------------------------


def _datalake_factory(settings: Settings, ctx: 'TaskContext') -> Any:
    from centraal_dataframework.resources import get_datalake

    return get_datalake(settings)


def _datalake_check(settings: Settings) -> List[str]:
    if settings.resolve_storage_connection_string() or settings.storage_account_name:
        return []
    return [
        f"Configure {settings.env_name('AZURE_STORAGE_CONNECTION_STRING')} "
        f"o {settings.env_name('STORAGE_ACCOUNT_NAME')}."
    ]


def _synapse_factory(settings: Settings, ctx: 'TaskContext') -> Any:
    from centraal_dataframework.synapse import SynapseClient

    return SynapseClient.from_settings(settings)


def _synapse_check(settings: Settings) -> List[str]:
    import importlib.util

    problems = []
    if not (settings.synapse_server or settings.synapse_workspace):
        problems.append(f"Configure {settings.env_name('SYNAPSE_WORKSPACE')} o {settings.env_name('SYNAPSE_SERVER')}.")
    if settings.synapse_auth == 'sql' and not (settings.synapse_user and settings.synapse_password):
        problems.append(
            f"{settings.env_name('SYNAPSE_AUTH')}=sql requiere {settings.env_name('SYNAPSE_USER')} "
            f"y {settings.env_name('SYNAPSE_PASSWORD')}."
        )
    if importlib.util.find_spec('pyodbc') is None:
        problems.append("Falta pyodbc: instale centraal-dataframework[synapse] y el 'ODBC Driver 18 for SQL Server'.")
    return problems


def _gx_check(settings: Settings) -> List[str]:
    import importlib.util

    if importlib.util.find_spec('great_expectations') is None:
        return ["Falta great_expectations: instale great-expectations>=1.3."]
    return []


def _gx_factory(settings: Settings, ctx: 'TaskContext') -> Any:
    from centraal_dataframework.resources import GreatExpectationsToolKit, get_context

    return GreatExpectationsToolKit(get_context(settings), ctx.name, settings)


register_resource('datalake', _datalake_factory, check=_datalake_check)
register_resource('synapse', _synapse_factory, check=_synapse_check)
register_resource('gx', _gx_factory, check=_gx_check)

__all__ = [
    'Resource',
    'register_resource',
    'unregister_resource',
    'get_resource',
    'registered_resources',
    'parse_use',
    'format_use',
    'normalize_uses',
    'settings_for',
    'check_uses',
]
