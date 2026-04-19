# src/utils/logger.py
"""Simple structured logger for pipeline stages."""

import logging
import json
from datetime import datetime, timezone


class PipelineLogger:
    def __init__(self, name: str):
        self._log = logging.getLogger(name)
        if not self._log.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(message)s"))
            self._log.addHandler(handler)
        self._log.setLevel(logging.INFO)

    def _emit(self, level: str, msg: str, **kwargs):
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "msg": msg,
            **kwargs,
        }
        getattr(self._log, level.lower())(json.dumps(payload))

    def info(self, msg: str, **kwargs):  self._emit("INFO",  msg, **kwargs)
    def warn(self, msg: str, **kwargs):  self._emit("WARNING", msg, **kwargs)
    def error(self, msg: str, **kwargs): self._emit("ERROR", msg, **kwargs)
