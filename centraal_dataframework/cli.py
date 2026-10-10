"""CLI para desarrollo local."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import timezone
from typing import Optional

from centraal_dataframework.config import load_framework_config
from centraal_dataframework.runner import Runner, reset_runner, set_runner
from centraal_dataframework.settings import clear_settings_cache, get_settings


def _build_runner(yaml_path: Optional[str] = None) -> Runner:
    clear_settings_cache()
    settings = get_settings()
    if yaml_path:
        settings.yaml_path = yaml_path  # type: ignore[misc]
    conf = load_framework_config(settings.yaml_path, allow_missing=False)
    runner = Runner(settings=settings, conf=conf, allow_missing_config=False)
    set_runner(runner)
    return runner


def cmd_validate_config(args: argparse.Namespace) -> int:
    """Valida el YAML."""
    path = args.config or get_settings().yaml_path
    conf = load_framework_config(path, allow_missing=False)
    print(f"OK: {path} — {len(conf.tareas)} tareas, {len(conf.emails_notificar)} emails.")
    for name, task in conf.tareas.items():
        deps = f" depends_on={task.depends_on}" if task.depends_on else ""
        cron = f" cron={task.cron}" if task.cron else f" dias={task.dias} horas={task.horas}"
        print(f"  - {name}: enabled={task.enabled}{cron}{deps}")
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    """Lista tareas registradas (requiere importar módulos de usuario)."""
    runner = _build_runner(args.config)
    if args.module:
        for mod in args.module:
            __import__(mod)
    tasks = runner.list_tasks()
    conf_tasks = sorted(runner.conf.tareas.keys())
    print("Registradas:", tasks or "(ninguna — importe módulos con --module)")
    print("En YAML:", conf_tasks)
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    """Ejecuta una tarea localmente."""
    runner = _build_runner(args.config)
    if args.module:
        for mod in args.module:
            __import__(mod)
    params = json.loads(args.params) if args.params else {}
    runner.run_task(args.task, params=params, notify=not args.no_notify)
    print(f"OK: {args.task}")
    return 0


def cmd_next_runs(args: argparse.Namespace) -> int:
    """Muestra próximas ejecuciones."""
    runner = _build_runner(args.config)
    task = args.task
    if task not in runner.conf.tareas:
        print(f"Tarea '{task}' no está en el YAML", file=sys.stderr)
        return 1
    runs = runner.next_runs(task, count=args.count)
    for dt in runs:
        print(dt.astimezone(timezone.utc).isoformat())
    return 0


def cmd_init(args: argparse.Namespace) -> int:
    """Genera plantilla de proyecto Azure Functions."""
    from pathlib import Path

    from centraal_dataframework import templates

    target = Path(args.path or '.').resolve()
    templates.scaffold(target, force=args.force)
    print(f"Plantilla creada en {target}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Construye parser CLI."""
    parser = argparse.ArgumentParser(prog='centraal-dataframework', description='CLI del framework Centraal')
    sub = parser.add_subparsers(dest='command', required=True)

    p_val = sub.add_parser('validate-config', help='Valida el YAML de configuración')
    p_val.add_argument('-c', '--config', help='Ruta al YAML')
    p_val.set_defaults(func=cmd_validate_config)

    p_list = sub.add_parser('list', help='Lista tareas')
    p_list.add_argument('-c', '--config', help='Ruta al YAML')
    p_list.add_argument('-m', '--module', action='append', help='Módulo a importar (puede repetirse)')
    p_list.set_defaults(func=cmd_list)

    p_run = sub.add_parser('run', help='Ejecuta una tarea')
    p_run.add_argument('task', help='Nombre de la tarea')
    p_run.add_argument('-c', '--config', help='Ruta al YAML')
    p_run.add_argument('-m', '--module', action='append', help='Módulo a importar')
    p_run.add_argument('--params', help='JSON de parámetros')
    p_run.add_argument('--no-notify', action='store_true', help='No enviar notificaciones')
    p_run.set_defaults(func=cmd_run)

    p_next = sub.add_parser('next-runs', help='Próximas ejecuciones de una tarea')
    p_next.add_argument('task', help='Nombre de la tarea')
    p_next.add_argument('-c', '--config', help='Ruta al YAML')
    p_next.add_argument('-n', '--count', type=int, default=5)
    p_next.set_defaults(func=cmd_next_runs)

    p_init = sub.add_parser('init', help='Scaffold de proyecto Function App')
    p_init.add_argument('path', nargs='?', default='.')
    p_init.add_argument('--force', action='store_true')
    p_init.set_defaults(func=cmd_init)

    return parser


def main(argv: Optional[list] = None) -> int:
    """Entry point CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    finally:
        reset_runner()


if __name__ == '__main__':
    sys.exit(main())
