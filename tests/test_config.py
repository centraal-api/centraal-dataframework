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


def test_silenciar_dq_requires_hasta_and_motivo():
    """El silencio de DQ exige fecha de vencimiento y motivo."""
    conf = TaskConfig(silenciar_dq={'hasta': '2026-11-15', 'motivo': 'Proveedor X, ticket #123'})
    assert str(conf.silenciar_dq.hasta) == '2026-11-15'
    for bad in [{'hasta': '2026-11-15'}, {'motivo': 'x'}, {'hasta': '2026-11-15', 'motivo': '  '}]:
        with pytest.raises(ValidationError):
            TaskConfig(silenciar_dq=bad)


def test_dq_warn_only_is_rejected_with_migration_hint():
    """dq_warn_only ya no existe: falla en vez de ignorarse en silencio."""
    with pytest.raises(ValidationError, match='silenciar_dq'):
        TaskConfig(dq_warn_only=True)


def test_silence_warnings():
    """Avisa silencios vencidos y los que vencen pronto; ignora los lejanos."""
    from datetime import date

    from centraal_dataframework.config import silence_warnings

    silencio = lambda hasta: {'silenciar_dq': {'hasta': hasta, 'motivo': 'm'}}  # noqa: E731
    conf = FrameworkConfig(
        tareas={
            'vencida': TaskConfig(**silencio('2026-10-28')),
            'pronto': TaskConfig(**silencio('2026-11-03')),
            'lejos': TaskConfig(**silencio('2026-12-31')),
            'sin': TaskConfig(),
        }
    )
    warnings = silence_warnings(conf, date(2026, 10, 29))
    assert len(warnings) == 2
    assert 'vencida' in warnings[0] and 'venció' in warnings[0]
    assert 'pronto' in warnings[1] and 'vence' in warnings[1]
