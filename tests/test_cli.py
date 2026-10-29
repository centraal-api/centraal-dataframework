"""Tests CLI."""

from centraal_dataframework.cli import main
from centraal_dataframework.templates import scaffold


def test_validate_config(yaml_file):
    """validate-config OK."""
    assert main(['validate-config', '-c', str(yaml_file)]) == 0


def test_next_runs(yaml_file):
    """next-runs imprime fechas."""
    assert main(['next-runs', 'alpha', '-c', str(yaml_file), '-n', '2']) == 0


def test_init(tmp_path):
    """init crea plantilla."""
    target = tmp_path / 'proj'
    assert main(['init', str(target)]) == 0
    assert (target / 'function_app.py').exists()
    assert (target / 'host.json').exists()
    assert (target / 'infra' / 'main.bicep').exists()


def test_scaffold_force(tmp_path):
    """scaffold no sobrescribe sin force."""
    scaffold(tmp_path)
    (tmp_path / 'function_app.py').write_text('keep', encoding='utf-8')
    scaffold(tmp_path, force=False)
    assert (tmp_path / 'function_app.py').read_text(encoding='utf-8') == 'keep'


def test_check_resources(yaml_file, tmp_path, monkeypatch, capsys):
    """check-resources falla si una tarea declara un recurso sin configurar."""
    monkeypatch.delenv('SYNAPSE_WORKSPACE', raising=False)
    monkeypatch.delenv('SYNAPSE_SERVER', raising=False)
    (tmp_path / 'mod_check_resources.py').write_text(
        'from centraal_dataframework import task\n\n\n'
        "@task(name='alpha', uses=['synapse'])\n"
        'def alpha(ctx):\n'
        '    """Tarea de prueba."""\n',
        encoding='utf-8',
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    assert main(['check-resources', '-c', str(yaml_file), '-m', 'mod_check_resources']) == 1
    out = capsys.readouterr().out
    assert 'alpha [synapse]: FALTA CONFIGURACIÓN' in out
    assert 'SYNAPSE_WORKSPACE' in out


def test_validate_config_warns_expired_silence(tmp_path, capsys):
    """validate-config avisa un silencio de DQ vencido sin fallar."""
    path = tmp_path / 'cfg.yaml'
    path.write_text(
        'tareas:\n  alpha:\n    silenciar_dq:\n      hasta: 2000-01-01\n      motivo: ticket 1\n', encoding='utf-8'
    )
    assert main(['validate-config', '-c', str(path)]) == 0
    assert 'AVISO: alpha: el silencio de DQ venció el 2000-01-01' in capsys.readouterr().out
