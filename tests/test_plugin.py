"""Unit tests for the panoptes log handler.

The tests stub out the HTTP layer so they run without Snakemake or a live
panoptes server.
"""

from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

from snakemake_interface_logger_plugins.common import LogEvent

from snakemake_logger_plugin_panoptes.log_handler import PanoptesLogHandler


class _Resp:
    def __init__(self, payload: Dict[str, Any], status: int = 200) -> None:
        self._payload = payload
        self.status_code = status

    def raise_for_status(self) -> None:
        if not 200 <= self.status_code < 300:
            raise RuntimeError(f"status {self.status_code}")

    def json(self) -> Dict[str, Any]:
        return self._payload


class _FakeSession:
    def __init__(self) -> None:
        self.get_calls: List[str] = []
        self.get_params: List[Any] = []
        self.post_calls: List[Dict[str, Any]] = []

    def get(self, url: str, params: Any = None, timeout: float = 0) -> _Resp:  # noqa: ARG002
        self.get_calls.append(url)
        self.get_params.append(params)
        return _Resp({"id": 7, "name": "wf-uuid"})

    def post(self, url: str, data: Dict[str, Any], timeout: float = 0) -> _Resp:  # noqa: ARG002
        self.post_calls.append({"url": url, "data": data})
        return _Resp({}, status=200)

    def close(self) -> None:
        pass


def _make_handler() -> PanoptesLogHandler:
    common = SimpleNamespace(dryrun=False)
    h = PanoptesLogHandler(common_settings=common, address="http://example.test")
    h.session = _FakeSession()  # type: ignore[assignment]
    return h


def _record(event: LogEvent, **attrs: Any) -> logging.LogRecord:
    record = logging.LogRecord(
        name="snakemake",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="",
        args=(),
        exc_info=None,
    )
    record.event = event  # type: ignore[attr-defined]
    for key, value in attrs.items():
        setattr(record, key, value)
    return record


def _last_message(handler: PanoptesLogHandler) -> Dict[str, Any]:
    fake = handler.session  # type: ignore[assignment]
    assert isinstance(fake, _FakeSession)
    assert fake.post_calls, "expected at least one POST"
    return json.loads(fake.post_calls[-1]["data"]["msg"])


def test_workflow_registration_uses_create_workflow() -> None:
    h = _make_handler()
    h.emit(_record(LogEvent.WORKFLOW_STARTED, snakefile="/tmp/Snakefile"))
    assert h.workflow_id == 7
    assert any("/create_workflow" in url for url in h.session.get_calls)  # type: ignore[attr-defined]
    msg = _last_message(h)
    assert msg["level"] == "info"


def test_job_info_payload_shape() -> None:
    h = _make_handler()
    h.emit(
        _record(
            LogEvent.JOB_INFO,
            jobid=3,
            rule_name="samtools_sort",
            input=["samples/c.bam"],
            output=["results/c.sorted.bam"],
            log=["logs/c.log"],
            wildcards={"sample": "c"},
            is_checkpoint=False,
            rule_msg=None,
            shellcmd="samtools sort ...",
            threads=4,
        )
    )
    msg = _last_message(h)
    assert msg["level"] == "job_info"
    assert msg["jobid"] == 3
    assert msg["name"] == "samtools_sort"
    assert msg["input"] == ["samples/c.bam"]
    assert msg["wildcards"] == {"sample": "c"}
    assert msg["is_checkpoint"] is False


def test_job_finished_accepts_job_id_or_jobid() -> None:
    h = _make_handler()
    h.emit(_record(LogEvent.JOB_FINISHED, job_id=5))
    assert _last_message(h) == {"level": "job_finished", "jobid": 5}

    h.emit(_record(LogEvent.JOB_FINISHED, jobid=6))
    assert _last_message(h) == {"level": "job_finished", "jobid": 6}


def test_progress_payload() -> None:
    h = _make_handler()
    h.emit(_record(LogEvent.PROGRESS, done=2, total=10))
    assert _last_message(h) == {"level": "progress", "done": 2, "total": 10}


def test_job_error_carries_rule_from_prior_job_info() -> None:
    h = _make_handler()
    h.emit(
        _record(
            LogEvent.JOB_INFO,
            jobid=11,
            rule_name="bwa_map",
            input=[],
            output=[],
            log=[],
            wildcards={},
            is_checkpoint=False,
        )
    )
    h.emit(_record(LogEvent.JOB_ERROR, jobid=11))
    msg = _last_message(h)
    assert msg == {"level": "job_error", "jobid": 11, "name": "bwa_map"}


def test_unknown_event_is_silently_ignored() -> None:
    h = _make_handler()
    rec = _record(LogEvent.RULEGRAPH, rulegraph={})
    h.emit(rec)
    # rulegraph isn't mapped — no POST should be sent (only the auto-registration GET).
    assert h.session.post_calls == []  # type: ignore[attr-defined]


def test_record_without_event_is_dropped() -> None:
    h = _make_handler()
    rec = logging.LogRecord("x", logging.INFO, "", 0, "msg", (), None)
    h.emit(rec)
    assert h.session.post_calls == []  # type: ignore[attr-defined]
    assert h.session.get_calls == []  # type: ignore[attr-defined]


def test_workflow_name_is_sent_as_query_param() -> None:
    common = SimpleNamespace(dryrun=False)
    h = PanoptesLogHandler(
        common_settings=common, address="http://example.test", name="my-run"
    )
    h.session = _FakeSession()  # type: ignore[assignment]
    h.emit(_record(LogEvent.WORKFLOW_STARTED, snakefile="/tmp/Snakefile"))
    assert h.session.get_params[-1] == {"name": "my-run"}  # type: ignore[attr-defined]


def test_no_name_sends_no_query_param() -> None:
    h = _make_handler()
    h.emit(_record(LogEvent.WORKFLOW_STARTED, snakefile="/tmp/Snakefile"))
    assert h.session.get_params[-1] is None  # type: ignore[attr-defined]


def test_run_info_sets_total() -> None:
    h = _make_handler()
    h.emit(_record(LogEvent.RUN_INFO, stats={"total": 14, "bwa": 7, "sort": 7}))
    msg = _last_message(h)
    assert msg == {"level": "progress", "done": 0, "total": 14}


def _ca_env_cleared(monkeypatch) -> None:
    for var in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE", "CURL_CA_BUNDLE"):
        monkeypatch.delenv(var, raising=False)


def test_ca_bundle_env_var_sets_session_verify(monkeypatch) -> None:
    _ca_env_cleared(monkeypatch)
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/etc/ssl/custom-ca.pem")
    common = SimpleNamespace(dryrun=False)
    h = PanoptesLogHandler(common_settings=common, address="https://example.test")
    assert h.session.verify == "/etc/ssl/custom-ca.pem"


def test_ca_bundle_precedence(monkeypatch) -> None:
    _ca_env_cleared(monkeypatch)
    monkeypatch.setenv("SSL_CERT_FILE", "/from/ssl-cert-file.pem")
    monkeypatch.setenv("CURL_CA_BUNDLE", "/from/curl-ca-bundle.pem")
    common = SimpleNamespace(dryrun=False)
    h = PanoptesLogHandler(common_settings=common, address="https://example.test")
    # SSL_CERT_FILE wins over CURL_CA_BUNDLE when REQUESTS_CA_BUNDLE is unset.
    assert h.session.verify == "/from/ssl-cert-file.pem"


def test_no_ca_bundle_leaves_default_verify(monkeypatch) -> None:
    _ca_env_cleared(monkeypatch)
    common = SimpleNamespace(dryrun=False)
    h = PanoptesLogHandler(common_settings=common, address="https://example.test")
    assert h.session.verify is True
