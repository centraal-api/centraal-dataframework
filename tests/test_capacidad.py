"""Tests del monitoreo de capacidad (una instancia, tareas en serie)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from centraal_dataframework import capacidad
from centraal_dataframework.capacidad import Medicion, evaluar
from centraal_dataframework.settings import Settings

AHORA = datetime(2026, 11, 2, 12, 0, tzinfo=timezone.utc)


def _medicion(**kw):
    base = dict(mensajes_en_cola=0, espera_max_min=0.0, memoria_pico_mb=500.0, memoria_limite_mb=3500.0)
    base.update(kw)
    return Medicion(**base)


def test_sin_presion_no_recomienda_nada():
    """Cola vacía y memoria holgada: una instancia alcanza."""
    assert evaluar(_medicion(), Settings()) == []


def test_cola_acumulada_recomienda_otra_instancia():
    """Muchos mensajes esperando: más instancias, si las tareas son independientes."""
    recs = evaluar(_medicion(mensajes_en_cola=6), Settings())
    assert len(recs) == 1
    assert 'instancia' in recs[0] and 'independientes' in recs[0]


def test_espera_larga_recomienda_otra_instancia():
    """El mensaje más viejo lleva más que el umbral esperando."""
    recs = evaluar(_medicion(espera_max_min=61), Settings())
    assert len(recs) == 1 and 'instancia' in recs[0]


def test_memoria_alta_recomienda_instancia_mas_grande_no_mas_instancias():
    """Una tarea que casi agota la memoria no se arregla con más instancias."""
    recs = evaluar(_medicion(memoria_pico_mb=3000), Settings())
    assert len(recs) == 1
    assert 'EP2' in recs[0]


def test_umbrales_configurables():
    """Los umbrales salen de Settings."""
    s = Settings(capacidad_cola_max=1, capacidad_espera_max_min=5, capacidad_memoria_pct=50)
    recs = evaluar(_medicion(mensajes_en_cola=2, espera_max_min=6, memoria_pico_mb=2000), s)
    assert len(recs) == 2  # cola/espera se reportan juntas; memoria aparte


def test_sin_datos_no_recomienda():
    """Sin medición de cola ni de límite de memoria (local, Windows) no hay recomendación."""
    m = Medicion(mensajes_en_cola=None, espera_max_min=None, memoria_pico_mb=None, memoria_limite_mb=None)
    assert evaluar(m, Settings()) == []


def test_memoria_limite_cgroup_v2(tmp_path, monkeypatch):
    """Lee el límite del contenedor (cgroup v2)."""
    f = tmp_path / 'memory.max'
    f.write_text(str(3584 * 1024 * 1024))
    monkeypatch.setattr(capacidad, '_CGROUP_FILES', (str(f),))
    assert capacidad.memoria_limite_mb() == pytest.approx(3584)


def test_memoria_limite_sin_limite(tmp_path, monkeypatch):
    """Sin límite de cgroup y sin /proc/meminfo: None."""
    f = tmp_path / 'memory.max'
    f.write_text('max')
    monkeypatch.setattr(capacidad, '_CGROUP_FILES', (str(f), str(tmp_path / 'no_existe')))
    monkeypatch.setattr(capacidad, '_MEMINFO', str(tmp_path / 'sin_meminfo'))
    assert capacidad.memoria_limite_mb() is None


def _fuentes(tmp_path, monkeypatch, cgroup=None, meminfo_kb=None):
    """Monta las dos fuentes en un sistema de archivos falso: el cgroup y /proc/meminfo."""
    f = tmp_path / 'memory.max'
    f.write_text('max' if cgroup is None else str(cgroup * 1024 * 1024))
    monkeypatch.setattr(capacidad, '_CGROUP_FILES', (str(f),))
    m = tmp_path / 'meminfo'
    if meminfo_kb is not None:
        m.write_text(f'MemTotal:       {meminfo_kb} kB\nMemFree:        1000 kB\n')
    monkeypatch.setattr(capacidad, '_MEMINFO', str(m))


def test_memoria_limite_cgroup_gana_sobre_meminfo(tmp_path, monkeypatch):
    """Con límite real de cgroup, ese manda aunque /proc/meminfo diga otra cosa."""
    _fuentes(tmp_path, monkeypatch, cgroup=3584, meminfo_kb=16_000_000)
    assert capacidad.memoria_limite_mb() == pytest.approx(3584)


def test_memoria_limite_sin_cgroup_usa_meminfo(tmp_path, monkeypatch):
    """App Service no fija límite de cgroup al contenedor: vale MemTotal de la VM (B1: 1,75 GB → cerca de 1750 MB)."""
    _fuentes(tmp_path, monkeypatch, cgroup=None, meminfo_kb=1_792_000)
    assert capacidad.memoria_limite_mb() == pytest.approx(1750)


def test_memoria_limite_cgroup_v1_sin_limite_usa_meminfo(tmp_path, monkeypatch):
    """El valor enorme de cgroup v1 también significa sin límite: se pasa a MemTotal."""
    f = tmp_path / 'memory.limit_in_bytes'
    f.write_text('9223372036854771712')
    monkeypatch.setattr(capacidad, '_CGROUP_FILES', (str(f),))
    m = tmp_path / 'meminfo'
    m.write_text('MemTotal:       1792000 kB\n')
    monkeypatch.setattr(capacidad, '_MEMINFO', str(m))
    assert capacidad.memoria_limite_mb() == pytest.approx(1750)


def test_estado_cola_cuenta_y_mensaje_mas_viejo():
    """Cuenta aproximada y espera del mensaje visible más viejo."""
    cliente = SimpleNamespace(
        get_queue_properties=lambda: SimpleNamespace(approximate_message_count=3),
        peek_messages=lambda max_messages: [SimpleNamespace(inserted_on=AHORA - timedelta(minutes=45))],
    )
    assert capacidad.estado_cola(cliente, ahora=AHORA) == (3, pytest.approx(45))


def test_estado_cola_vacia():
    """Cola vacía: espera 0."""
    cliente = SimpleNamespace(
        get_queue_properties=lambda: SimpleNamespace(approximate_message_count=0),
        peek_messages=lambda max_messages: [],
    )
    assert capacidad.estado_cola(cliente, ahora=AHORA) == (0, 0.0)


def test_medir_tolera_cola_inaccesible(monkeypatch):
    """Si no se puede leer la cola, mide lo demás y deja la cola en None."""

    def falla(*a, **k):
        raise RuntimeError('sin acceso')

    monkeypatch.setattr(capacidad, 'cliente_cola', falla)
    m = capacidad.medir(Settings())
    assert m.mensajes_en_cola is None and m.espera_max_min is None


def test_espera_de_mensaje():
    """Minutos entre que se encoló el mensaje y que empezó a ejecutarse."""
    msg = SimpleNamespace(insertion_time=AHORA - timedelta(minutes=12))
    assert capacidad.espera_de_mensaje(msg, ahora=AHORA) == pytest.approx(12)
    assert capacidad.espera_de_mensaje(SimpleNamespace(insertion_time=None), ahora=AHORA) is None
