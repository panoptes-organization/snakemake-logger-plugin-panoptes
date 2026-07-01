"""HTTP log handler that forwards Snakemake LogEvents to a panoptes server."""

from __future__ import annotations

import json
import logging
import os
import time
from logging import Handler, LogRecord
from typing import Any, Dict, Optional

import requests
from snakemake_interface_logger_plugins.common import LogEvent
from snakemake_interface_logger_plugins.settings import OutputSettingsLoggerInterface


_LOG = logging.getLogger(__name__)


def _to_jsonable(value: Any) -> Any:
    """Best-effort conversion to JSON-serialisable primitives."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(v) for v in value]
    # Snakemake's Namedlist / IOFile / Wildcards behave like sequences/mappings;
    # fall back to repr() so the panoptes server still sees something.
    try:
        return list(value)  # type: ignore[arg-type]
    except TypeError:
        return str(value)


def _resources_to_dict(resources: Any) -> Dict[str, Any]:
    if resources is None:
        return {}
    names = getattr(resources, "_names", None)
    if names is None:
        return _to_jsonable(resources) if isinstance(resources, dict) else {}
    return {
        name: _to_jsonable(value)
        for name, value in zip(names, resources)
        if name not in {"_cores", "_nodes"}
    }


class PanoptesLogHandler(Handler):
    """Translate Snakemake LogRecords into panoptes HTTP API calls."""

    def __init__(
        self,
        common_settings: OutputSettingsLoggerInterface,
        address: str,
        timeout: float = 10.0,
        workflow_id: Optional[str] = None,
    ):
        super().__init__()
        self.common_settings = common_settings
        self.address = address.rstrip("/")
        self.timeout = timeout
        # User-supplied stable id (distinct from the panoptes-assigned numeric
        # id stored in self.workflow_id once the workflow is registered).
        self.requested_workflow_id = workflow_id
        self.session = requests.Session()
        # Honour the standard CA-bundle env vars so HTTPS panoptes servers
        # behind a private/corporate CA verify correctly.
        ca_bundle = (
            os.environ.get("REQUESTS_CA_BUNDLE")
            or os.environ.get("SSL_CERT_FILE")
            or os.environ.get("CURL_CA_BUNDLE")
        )
        if ca_bundle:
            self.session.verify = ca_bundle
        self.workflow_id: Optional[int] = None
        self.workflow_name: Optional[str] = None
        # Map (Snakemake-side) jobid -> rule name, so we can re-attach context
        # to events like JOB_FINISHED / JOB_ERROR that don't carry the rule.
        self._job_rules: Dict[int, str] = {}
        # Whether an error was seen during the run, and whether we already told
        # panoptes the workflow finished (see close()).
        self._errored = False
        self._success_reported = False

    # ------------------------------------------------------------------ #
    # registration with panoptes
    # ------------------------------------------------------------------ #

    def _ensure_workflow(self) -> bool:
        if self.workflow_id is not None:
            return True
        try:
            params = (
                {"workflow_id": self.requested_workflow_id}
                if self.requested_workflow_id
                else None
            )
            response = self.session.get(
                f"{self.address}/create_workflow", params=params, timeout=self.timeout
            )
            response.raise_for_status()
            payload = response.json()
            self.workflow_id = payload.get("id")
            self.workflow_name = payload.get("name")
            return self.workflow_id is not None
        except Exception as exc:
            _LOG.warning("panoptes: could not register workflow: %s", exc)
            return False

    def _post_message(self, message: Dict[str, Any]) -> None:
        if not self._ensure_workflow():
            return
        payload = {
            "msg": json.dumps(message),
            "timestamp": time.asctime(),
            "id": self.workflow_id,
        }
        try:
            self.session.post(
                f"{self.address}/update_workflow_status",
                data=payload,
                timeout=self.timeout,
            )
        except Exception as exc:
            _LOG.warning("panoptes: failed to deliver event %r: %s", message.get("level"), exc)

    # ------------------------------------------------------------------ #
    # event translation
    # ------------------------------------------------------------------ #

    def _on_workflow_started(self, record: LogRecord) -> Optional[Dict[str, Any]]:
        # Make sure panoptes has a workflow row before any other event lands.
        self._ensure_workflow()
        snakefile = getattr(record, "snakefile", None)
        return {
            "level": "info",
            "msg": f"Workflow started: {snakefile}" if snakefile else "Workflow started",
        }

    def _on_job_info(self, record: LogRecord) -> Dict[str, Any]:
        jobid = int(getattr(record, "jobid", 0) or 0)
        rule_name = getattr(record, "rule_name", "") or ""
        self._job_rules[jobid] = rule_name
        return {
            "level": "job_info",
            "jobid": jobid,
            "name": rule_name,
            "msg": getattr(record, "rule_msg", None),
            "input": _to_jsonable(getattr(record, "input", []) or []),
            "output": _to_jsonable(getattr(record, "output", []) or []),
            "log": _to_jsonable(getattr(record, "log", []) or []),
            "wildcards": _to_jsonable(getattr(record, "wildcards", {}) or {}),
            "is_checkpoint": bool(getattr(record, "is_checkpoint", False)),
            "shellcmd": getattr(record, "shellcmd", None),
            "threads": getattr(record, "threads", None),
            "priority": getattr(record, "priority", None),
            "reason": getattr(record, "reason", None),
            "resources": _resources_to_dict(getattr(record, "resources", None)),
        }

    def _on_job_started(self, record: LogRecord) -> Optional[Dict[str, Any]]:
        jobs = getattr(record, "jobs", None)
        if jobs is None:
            return None
        if isinstance(jobs, int):
            jobs = [jobs]
        return {"level": "job_started", "jobs": [int(j) for j in jobs]}

    def _on_job_finished(self, record: LogRecord) -> Optional[Dict[str, Any]]:
        jobid = getattr(record, "job_id", None)
        if jobid is None:
            jobid = getattr(record, "jobid", None)
        if jobid is None:
            return None
        return {"level": "job_finished", "jobid": int(jobid)}

    def _on_job_error(self, record: LogRecord) -> Dict[str, Any]:
        self._errored = True
        jobid = int(getattr(record, "jobid", 0) or 0)
        return {
            "level": "job_error",
            "jobid": jobid,
            "name": self._job_rules.get(jobid, ""),
        }

    def _on_shellcmd(self, record: LogRecord) -> Optional[Dict[str, Any]]:
        shellcmd = getattr(record, "shellcmd", None)
        if not shellcmd:
            return None
        jobid = getattr(record, "jobid", None)
        return {
            "level": "shellcmd",
            "jobid": int(jobid) if jobid is not None else None,
            "msg": shellcmd,
        }

    def _on_progress(self, record: LogRecord) -> Dict[str, Any]:
        return {
            "level": "progress",
            "done": int(getattr(record, "done", 0) or 0),
            "total": int(getattr(record, "total", 0) or 0),
        }

    def _on_error(self, record: LogRecord) -> Dict[str, Any]:
        self._errored = True
        return {
            "level": "error",
            "msg": getattr(record, "exception", None) or record.getMessage(),
            "rule": getattr(record, "rule", None),
            "location": getattr(record, "location", None),
            "traceback": getattr(record, "traceback", None),
        }

    def _on_run_info(self, record: LogRecord) -> Optional[Dict[str, Any]]:
        stats = getattr(record, "stats", None) or {}
        total = stats.get("total")
        if total is None:
            return None
        return {"level": "progress", "done": 0, "total": int(total)}

    # ------------------------------------------------------------------ #
    # Handler interface
    # ------------------------------------------------------------------ #

    _DISPATCH = {
        LogEvent.WORKFLOW_STARTED: "_on_workflow_started",
        LogEvent.JOB_INFO: "_on_job_info",
        LogEvent.JOB_STARTED: "_on_job_started",
        LogEvent.JOB_FINISHED: "_on_job_finished",
        LogEvent.JOB_ERROR: "_on_job_error",
        LogEvent.SHELLCMD: "_on_shellcmd",
        LogEvent.PROGRESS: "_on_progress",
        LogEvent.ERROR: "_on_error",
        LogEvent.RUN_INFO: "_on_run_info",
    }

    def emit(self, record: LogRecord) -> None:
        try:
            event = getattr(record, "event", None)
            if event is None:
                return
            method_name = self._DISPATCH.get(event)
            if method_name is None:
                return
            message = getattr(self, method_name)(record)
            if message is None:
                return
            self._post_message(message)
        except Exception:
            self.handleError(record)

    def _report_success_if_needed(self) -> None:
        # Snakemake has no "workflow finished" LogEvent, and panoptes marks a
        # workflow Done only when it sees progress with done == total. With
        # `--until`, Snakemake keeps reporting the full-DAG total while running
        # only a subset, so done never reaches total and the workflow would
        # stay "Running" forever. Signal completion explicitly at shutdown for a
        # real run that registered a workflow and saw no error, so panoptes can
        # reconcile and mark it Done. Dry runs are skipped (nothing executed).
        if (
            self._success_reported
            or self.workflow_id is None
            or self._errored
            or getattr(self.common_settings, "dryrun", False)
        ):
            return
        self._success_reported = True
        self._post_message({"level": "workflow_success"})

    def close(self) -> None:
        try:
            self._report_success_if_needed()
        finally:
            try:
                self.session.close()
            finally:
                super().close()
