"""Historial de ejecuciones en Azure Table Storage."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from centraal_dataframework.settings import Settings

logger = logging.getLogger(__name__)


class RunHistory:
    """Persiste runs en Azure Table Storage (opcional)."""

    def __init__(self, settings: Settings):
        """Inicializa cliente de tablas si HISTORY_ENABLED."""
        self.settings = settings
        self._table = None
        if not settings.history_enabled:
            return
        conn = settings.resolve_storage_connection_string()
        if not conn:
            logger.warning("HISTORY_ENABLED pero no hay connection string; historial desactivado.")
            return
        try:
            from azure.data.tables import TableServiceClient

            service = TableServiceClient.from_connection_string(conn)
            self._table = service.create_table_if_not_exists(settings.history_table_name)
        except Exception:  # noqa: BLE001
            logger.exception("No se pudo inicializar historial de tablas.")
            self._table = None

    @property
    def enabled(self) -> bool:
        """Indica si el historial está activo."""
        return self._table is not None

    def record_start(self, task_name: str, run_id: str, params: Dict[str, Any]) -> None:
        """Registra inicio."""
        self._upsert(
            {
                'PartitionKey': task_name,
                'RowKey': run_id,
                'status': 'running',
                'started_at': datetime.now(timezone.utc).isoformat(),
                'params': str(params)[:2000],
            }
        )

    def record_success(self, task_name: str, run_id: str, started: datetime, result: Any = None) -> None:
        """Registra éxito."""
        ended = datetime.now(timezone.utc)
        duration = (ended - started).total_seconds()
        self._upsert(
            {
                'PartitionKey': task_name,
                'RowKey': run_id,
                'status': 'success',
                'started_at': started.isoformat(),
                'ended_at': ended.isoformat(),
                'duration_seconds': duration,
            }
        )

    def record_failure(
        self,
        task_name: str,
        run_id: str,
        started: datetime,
        error: Exception,
        url: str = '',
    ) -> None:
        """Registra fallo."""
        ended = datetime.now(timezone.utc)
        duration = (ended - started).total_seconds()
        self._upsert(
            {
                'PartitionKey': task_name,
                'RowKey': run_id,
                'status': 'failed',
                'started_at': started.isoformat(),
                'ended_at': ended.isoformat(),
                'duration_seconds': duration,
                'error': str(error)[:2000],
                'url': url[:2000],
            }
        )

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Busca un run por id (escaneo limitado)."""
        if not self._table:
            return None
        try:
            for entity in self._table.list_entities():
                if entity.get('RowKey') == run_id:
                    return dict(entity)
        except Exception:  # noqa: BLE001
            logger.exception("Error consultando historial")
        return None

    def list_runs(self, task_name: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """Lista runs recientes."""
        if not self._table:
            return []
        rows: List[Dict[str, Any]] = []
        try:
            if task_name:
                entities = self._table.query_entities(f"PartitionKey eq '{task_name}'")
            else:
                entities = self._table.list_entities()
            for entity in entities:
                rows.append(dict(entity))
                if len(rows) >= limit:
                    break
        except Exception:  # noqa: BLE001
            logger.exception("Error listando historial")
        return rows

    def has_successful_run_in_window(self, task_name: str, window_key: str) -> bool:
        """Idempotencia simple: RowKey = window_key."""
        if not self._table:
            return False
        try:
            entity = self._table.get_entity(partition_key=task_name, row_key=window_key)
            return entity.get('status') == 'success'
        except Exception:  # noqa: BLE001
            return False

    def _upsert(self, entity: Dict[str, Any]) -> None:
        if not self._table:
            return
        try:
            from azure.data.tables import UpdateMode

            self._table.upsert_entity(entity=entity, mode=UpdateMode.MERGE)
        except Exception:  # noqa: BLE001
            logger.exception("No se pudo persistir historial")
