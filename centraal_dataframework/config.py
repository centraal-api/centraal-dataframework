"""Modelos pydantic para el YAML de configuración."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


def _parse_csv_int_or_star(value: Union[str, int, List[Any]]) -> Union[str, List[int]]:
    """Normaliza dias/horas: '*' o lista de enteros."""
    if value == '*' or value == ['*']:
        return '*'
    if isinstance(value, int):
        return [value]
    if isinstance(value, list):
        out: List[int] = []
        for item in value:
            if str(item).strip() == '*':
                return '*'
            out.append(int(str(item).strip()))
        return out
    text = str(value).strip()
    if text == '*':
        return '*'
    return [int(part.strip()) for part in text.split(',') if part.strip()]


class TaskConfig(BaseModel):
    """Configuración de una tarea en el YAML."""

    dias: Union[str, List[int]] = '*'
    horas: Union[str, List[int]] = '*'
    emails: Optional[List[str]] = None
    enabled: bool = True
    cron: Optional[str] = None
    depends_on: List[str] = Field(default_factory=list)
    params: Dict[str, Any] = Field(default_factory=dict)
    timezone: Optional[str] = None
    notify_on_success: Optional[bool] = None
    dq_warn_only: bool = False

    @field_validator('dias', 'horas', mode='before')
    @classmethod
    def _normalize_schedule(cls, value: Any) -> Union[str, List[int]]:
        return _parse_csv_int_or_star(value)

    @field_validator('depends_on', mode='before')
    @classmethod
    def _normalize_depends(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        return list(value)


class FrameworkConfig(BaseModel):
    """Configuración global del framework."""

    emails_notificar: List[str] = Field(default_factory=list)
    tareas: Dict[str, TaskConfig] = Field(default_factory=dict)
    # legado: ignorar si viene en YAML antiguo
    url_logicapp_email: Optional[str] = Field(default=None, exclude=True)

    @model_validator(mode='after')
    def _validate_depends_on(self) -> FrameworkConfig:
        known = set(self.tareas.keys())
        for name, task in self.tareas.items():
            for dep in task.depends_on:
                if dep not in known:
                    raise ValueError(f"Tarea '{name}' depende de '{dep}' que no existe en configuración.")
        _assert_no_cycles(self.tareas)
        return self


def _assert_no_cycles(tareas: Dict[str, TaskConfig]) -> None:
    """Detecta ciclos en depends_on."""
    visiting: Set[str] = set()
    visited: Set[str] = set()

    def dfs(node: str) -> None:
        if node in visiting:
            raise ValueError(f"Ciclo detectado en depends_on involucrando '{node}'.")
        if node in visited:
            return
        visiting.add(node)
        for dep in tareas[node].depends_on:
            dfs(dep)
        visiting.remove(node)
        visited.add(node)

    for name in tareas:
        dfs(name)


def load_framework_config(path: Union[str, Path], *, allow_missing: bool = False) -> FrameworkConfig:
    """Carga y valida el YAML de configuración."""
    path = Path(path)
    if not path.exists():
        if allow_missing:
            return FrameworkConfig()
        raise FileNotFoundError(
            f"No se encontró el archivo de configuración '{path}'. "
            "Defina YAML_CONFIGURACION o cree centraal_dataframework.yaml."
        )
    with path.open('r', encoding='utf-8') as file:
        data = yaml.safe_load(file) or {}
    if not isinstance(data, dict):
        raise ValueError(f"El YAML '{path}' debe ser un objeto/mapping en la raíz.")
    return FrameworkConfig.model_validate(data)
