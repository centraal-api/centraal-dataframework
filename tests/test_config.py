"""Tests de config YAML."""

import pytest
from pydantic import ValidationError

from centraal_dataframework.config import FrameworkConfig, TaskConfig, load_framework_config


def test_normalize_dias_horas_with_spaces():
    """Strip y enteros en dias/horas."""
    conf = TaskConfig(dias=' 0, 2 ,3 ', horas='08, 12')
    assert conf.dias == [0, 2, 3]
    assert conf.horas == [8, 12]


def test_star_schedule():
    """'*' se preserva."""
    conf = TaskConfig(dias='*', horas='*')
    assert conf.dias == '*'
    assert conf.horas == '*'


def test_cycle_depends_on_raises():
    """Ciclos en depends_on fallan al validar."""
    with pytest.raises(ValidationError):
        FrameworkConfig(
            tareas={
                'a': TaskConfig(depends_on=['b']),
                'b': TaskConfig(depends_on=['a']),
            }
        )


def test_missing_dependency_raises():
    """Dependencia inexistente falla."""
    with pytest.raises(ValidationError):
        FrameworkConfig(tareas={'a': TaskConfig(depends_on=['missing'])})


def test_load_framework_config(yaml_file):
    """Carga YAML válido."""
    conf = load_framework_config(yaml_file)
    assert 'alpha' in conf.tareas
    assert conf.emails_notificar


def test_load_missing_raises(tmp_path):
    """Archivo faltante sin allow_missing."""
    with pytest.raises(FileNotFoundError):
        load_framework_config(tmp_path / 'nope.yaml', allow_missing=False)


def test_load_missing_allowed(tmp_path):
    """allow_missing devuelve vacío."""
    conf = load_framework_config(tmp_path / 'nope.yaml', allow_missing=True)
    assert conf.tareas == {}
