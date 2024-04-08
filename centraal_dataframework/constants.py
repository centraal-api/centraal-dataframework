"""Constantes usadas a lo largo de la libreria."""

import os
import datetime

TASK_PARAM_NAME = "task_info"

UTC_OFFSET = datetime.timedelta(hours=os.environ.get("UTC_OFFSET", -5))
STR_FMT = 'TAREA: %(name)s--%(asctime)s-%(levelname)s-%(message)s'
LOCAL = "local"
