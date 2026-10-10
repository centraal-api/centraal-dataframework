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
