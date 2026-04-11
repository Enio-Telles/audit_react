from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from utilitarios.project_paths import APP_STATE_ROOT


@dataclass(frozen=True)
class SchemaVersionSnapshot:
    table_name: str
    version: int
    schema_hash: str
    fields: dict[str, str]
    recorded_at: str
    source_path: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class SchemaRegistry:
    """Registro simples de versões de schema em JSON.

    É uma etapa intermediária de governança antes de uma eventual adoção de Delta Lake.
    """

    def __init__(self, registry_path: Path | None = None) -> None:
        self.registry_path = registry_path or (APP_STATE_ROOT / "schema_registry.json")
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)

    def _load(self) -> dict[str, list[dict[str, Any]]]:
        if not self.registry_path.exists():
            return {}
        try:
            payload = json.loads(self.registry_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _save(self, payload: dict[str, list[dict[str, Any]]]) -> None:
        self.registry_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _normalize_schema(schema: Mapping[str, Any]) -> dict[str, str]:
        return {str(key): str(value) for key, value in schema.items()}

    @staticmethod
    def _hash_schema(fields: Mapping[str, str]) -> str:
        payload = json.dumps(fields, sort_keys=True, ensure_ascii=False)
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()

    def list_versions(self, table_name: str) -> list[SchemaVersionSnapshot]:
        payload = self._load()
        versions = payload.get(table_name, [])
        result: list[SchemaVersionSnapshot] = []
        for item in versions:
            try:
                result.append(SchemaVersionSnapshot(**item))
            except TypeError:
                continue
        return result

    def latest_snapshot(self, table_name: str) -> SchemaVersionSnapshot | None:
        versions = self.list_versions(table_name)
        return versions[-1] if versions else None

    def find_latest_by_source_path(self, source_path: str | None) -> SchemaVersionSnapshot | None:
        if not source_path:
            return None
        payload = self._load()
        normalized = str(source_path)
        candidates: list[SchemaVersionSnapshot] = []
        for versions in payload.values():
            for item in versions:
                try:
                    snapshot = SchemaVersionSnapshot(**item)
                except TypeError:
                    continue
                if snapshot.source_path == normalized:
                    candidates.append(snapshot)
        if not candidates:
            return None
        candidates.sort(key=lambda item: (item.recorded_at, item.version))
        return candidates[-1]

    def record_schema(
        self,
        table_name: str,
        schema: Mapping[str, Any],
        *,
        source_path: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SchemaVersionSnapshot:
        payload = self._load()
        existing = self.list_versions(table_name)
        normalized = self._normalize_schema(schema)
        schema_hash = self._hash_schema(normalized)

        if existing and existing[-1].schema_hash == schema_hash:
            return existing[-1]

        snapshot = SchemaVersionSnapshot(
            table_name=table_name,
            version=(existing[-1].version + 1) if existing else 1,
            schema_hash=schema_hash,
            fields=normalized,
            recorded_at=datetime.now(timezone.utc).isoformat(),
            source_path=source_path,
            metadata=metadata or {},
        )
        payload.setdefault(table_name, []).append(asdict(snapshot))
        self._save(payload)
        return snapshot

    def diff_latest(self, table_name: str, schema: Mapping[str, Any]) -> dict[str, list[str]]:
        versions = self.list_versions(table_name)
        current = self._normalize_schema(schema)
        if not versions:
            return {"added": sorted(current.keys()), "removed": [], "type_changed": []}

        latest = versions[-1].fields
        added = sorted([column for column in current if column not in latest])
        removed = sorted([column for column in latest if column not in current])
        type_changed = sorted(
            [column for column in current if column in latest and current[column] != latest[column]]
        )
        return {"added": added, "removed": removed, "type_changed": type_changed}

    def validate_required_columns(self, table_name: str, columns: list[str]) -> tuple[bool, list[str]]:
        versions = self.list_versions(table_name)
        if not versions:
            return True, []
        latest_columns = set(versions[-1].fields.keys())
        provided = set(columns)
        missing = sorted(latest_columns - provided)
        return len(missing) == 0, missing

    def summary(self) -> dict[str, Any]:
        payload = self._load()
        table_names = sorted(payload.keys())
        latest_versions: dict[str, dict[str, Any]] = {}
        total_versions = 0

        for table_name in table_names:
            snapshots = self.list_versions(table_name)
            total_versions += len(snapshots)
            latest = snapshots[-1] if snapshots else None
            if latest is None:
                continue
            latest_versions[table_name] = {
                "version": latest.version,
                "schema_hash": latest.schema_hash,
                "field_count": len(latest.fields),
                "recorded_at": latest.recorded_at,
                "source_path": latest.source_path,
                "metadata": latest.metadata,
            }

        return {
            "registry_path": str(self.registry_path),
            "total_tables": len(table_names),
            "total_versions": total_versions,
            "tables": table_names,
            "latest_versions": latest_versions,
        }
