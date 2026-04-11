from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

LOGGER = logging.getLogger(__name__)
PRODUCER = "https://github.com/Enio-Telles/audit_react"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class LineageDataset:
    namespace: str
    name: str

    def as_dict(self) -> dict[str, Any]:
        return {"namespace": self.namespace, "name": self.name, "facets": {}}


class OpenLineageEmitter:
    def __init__(self, *, namespace: str | None = None, job_name: str = "audit_react.pipeline") -> None:
        self.endpoint = os.getenv("OPENLINEAGE_URL", "").strip()
        if self.endpoint and self.endpoint.endswith("/"):
            self.endpoint = self.endpoint.rstrip("/")
        self.namespace = namespace or os.getenv("OPENLINEAGE_NAMESPACE", "audit_react")
        self.job_name = job_name
        self.run_id = str(uuid.uuid4())
        self.enabled = bool(self.endpoint)

    def status(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "endpoint": self.endpoint or None,
            "namespace": self.namespace,
            "job_name": self.job_name,
            "run_id": self.run_id,
        }

    def _emit(self, payload: dict[str, Any]) -> None:
        if not self.enabled:
            return
        request = Request(
            self.endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=5) as response:
                response.read()
        except URLError as exc:
            LOGGER.warning("Falha ao emitir OpenLineage para %s: %s", self.endpoint, exc)

    def emit_event(
        self,
        event_type: str,
        *,
        job_name: str | None = None,
        inputs: list[LineageDataset] | None = None,
        outputs: list[LineageDataset] | None = None,
        facets: dict[str, Any] | None = None,
    ) -> None:
        payload = {
            "eventType": event_type,
            "eventTime": _utc_now(),
            "run": {"runId": self.run_id},
            "job": {
                "namespace": self.namespace,
                "name": job_name or self.job_name,
                "facets": {},
            },
            "producer": PRODUCER,
            "inputs": [item.as_dict() for item in inputs or []],
            "outputs": [item.as_dict() for item in outputs or []],
            "facets": facets or {},
        }
        self._emit(payload)

    def start_run(self, *, cnpj: str, inputs: list[LineageDataset] | None = None) -> None:
        self.emit_event(
            "START",
            facets={"run": {"cnpj": cnpj}},
            inputs=inputs,
        )

    def complete_run(self, *, outputs: list[LineageDataset] | None = None, success: bool = True) -> None:
        self.emit_event("COMPLETE" if success else "FAIL", outputs=outputs)

    def emit_step_complete(
        self,
        step_name: str,
        *,
        inputs: list[LineageDataset] | None = None,
        outputs: list[LineageDataset] | None = None,
        duration_seconds: float | None = None,
    ) -> None:
        facets = {"step": {"duration_seconds": duration_seconds}} if duration_seconds is not None else None
        self.emit_event("COMPLETE", job_name=f"{self.job_name}.{step_name}", inputs=inputs, outputs=outputs, facets=facets)

    def emit_step_fail(self, step_name: str, *, error_message: str) -> None:
        self.emit_event(
            "FAIL",
            job_name=f"{self.job_name}.{step_name}",
            facets={"error": {"message": error_message}},
        )


def get_openlineage_emitter(job_name: str = "audit_react.pipeline") -> OpenLineageEmitter:
    return OpenLineageEmitter(job_name=job_name)
