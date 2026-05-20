"""Snakemake logger plugin that forwards workflow events to a panoptes server."""

from dataclasses import dataclass, field
from logging import LogRecord
from typing import Optional

from snakemake_interface_logger_plugins.base import LogHandlerBase
from snakemake_interface_logger_plugins.settings import LogHandlerSettingsBase

from snakemake_logger_plugin_panoptes.log_handler import PanoptesLogHandler


@dataclass
class LogHandlerSettings(LogHandlerSettingsBase):
    """Settings exposed via ``--logger-panoptes-*`` on the Snakemake CLI."""

    address: Optional[str] = field(
        default=None,
        metadata={
            "help": (
                "Base URL of the panoptes server, e.g. http://127.0.0.1:5000. "
                "Required."
            ),
            "env_var": True,
            "required": True,
        },
    )
    timeout: Optional[float] = field(
        default=10.0,
        metadata={
            "help": "Per-request HTTP timeout in seconds.",
            "env_var": False,
            "required": False,
        },
    )


class LogHandler(LogHandlerBase, PanoptesLogHandler):
    """Snakemake logger plugin entry point."""

    def __post_init__(self) -> None:
        # ``self.settings`` and ``self.common_settings`` are populated by
        # the Snakemake plugin runtime before __post_init__ is called.
        settings: LogHandlerSettings = self.settings  # type: ignore[assignment]
        if not settings.address:
            raise ValueError(
                "snakemake-logger-plugin-panoptes requires --logger-panoptes-address "
                "(or the SNAKEMAKE_LOGGER_PANOPTES_ADDRESS env var) to be set."
            )
        PanoptesLogHandler.__init__(
            self,
            common_settings=self.common_settings,
            address=settings.address,
            timeout=settings.timeout if settings.timeout is not None else 10.0,
        )

    def emit(self, record: LogRecord) -> None:
        PanoptesLogHandler.emit(self, record)

    @property
    def writes_to_stream(self) -> bool:
        return False

    @property
    def writes_to_file(self) -> bool:
        return False

    @property
    def has_filter(self) -> bool:
        return False

    @property
    def has_formatter(self) -> bool:
        return False

    @property
    def needs_rulegraph(self) -> bool:
        return False


__all__ = ["LogHandler", "LogHandlerSettings"]
