from __future__ import annotations

import hashlib
import json
import threading
import time
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from utilitarios.project_paths import APP_STATE_ROOT


@dataclass
class CachedSqlEntry:
    sql_id: str
    path: str
    sql_text: str
    checksum: str
    loaded_at: float
    access_count: int = 0
    last_accessed: float = field(default_factory=time.time)

    def is_expired(self, ttl_seconds: float) -> bool:
        return (time.time() - self.loaded_at) > ttl_seconds


class SqlCatalogCache:
    """Cache L1/L2 para conteúdo SQL.

    L1: memória com política LRU.
    L2: persistência em disco dentro de workspace/app_state/sql_cache.
    """

    def __init__(
        self,
        *,
        max_memory_entries: int = 256,
        ttl_seconds: float = 3600.0,
        cache_root: Path | None = None,
    ) -> None:
        self.max_memory_entries = max_memory_entries
        self.ttl_seconds = ttl_seconds
        self.cache_root = cache_root or (APP_STATE_ROOT / "sql_cache")
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self._manifest_path = self.cache_root / "manifest.json"
        self._memory: OrderedDict[str, CachedSqlEntry] = OrderedDict()
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
        self._load_manifest()

    @staticmethod
    def _cache_filename(sql_id: str) -> str:
        digest = hashlib.sha1(sql_id.lower().encode("utf-8")).hexdigest()
        return f"{digest}.json"

    @staticmethod
    def _compute_checksum(path: Path) -> str:
        payload = path.read_bytes()
        return hashlib.sha1(payload).hexdigest()

    def _load_manifest(self) -> None:
        if not self._manifest_path.exists():
            return
        try:
            payload = json.loads(self._manifest_path.read_text(encoding="utf-8"))
        except Exception:
            return
        entries = payload.get("entries", [])
        for raw in entries:
            try:
                entry = CachedSqlEntry(**raw)
            except TypeError:
                continue
            if entry.is_expired(self.ttl_seconds):
                continue
            self._put_memory(entry)

    def _save_manifest(self) -> None:
        payload = {
            "entries": [asdict(entry) for entry in self._memory.values()],
            "updated_at": time.time(),
            "hits": self._hits,
            "misses": self._misses,
        }
        self._manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _put_memory(self, entry: CachedSqlEntry) -> None:
        key = entry.sql_id.lower()
        self._memory[key] = entry
        self._memory.move_to_end(key)
        while len(self._memory) > self.max_memory_entries:
            self._memory.popitem(last=False)

    def _disk_path(self, sql_id: str) -> Path:
        return self.cache_root / self._cache_filename(sql_id)

    def get(self, sql_id: str, path: Path) -> str | None:
        key = sql_id.lower()
        with self._lock:
            entry = self._memory.get(key)
            if entry and not entry.is_expired(self.ttl_seconds):
                current_checksum = self._compute_checksum(path)
                if current_checksum == entry.checksum:
                    entry.access_count += 1
                    entry.last_accessed = time.time()
                    self._memory.move_to_end(key)
                    self._hits += 1
                    return entry.sql_text
                self._memory.pop(key, None)

            disk_path = self._disk_path(sql_id)
            if disk_path.exists():
                try:
                    raw = json.loads(disk_path.read_text(encoding="utf-8"))
                    disk_entry = CachedSqlEntry(**raw)
                    current_checksum = self._compute_checksum(path)
                    if (not disk_entry.is_expired(self.ttl_seconds)) and current_checksum == disk_entry.checksum:
                        disk_entry.access_count += 1
                        disk_entry.last_accessed = time.time()
                        self._put_memory(disk_entry)
                        self._hits += 1
                        self._save_manifest()
                        return disk_entry.sql_text
                except Exception:
                    pass

            self._misses += 1
            return None

    def set(self, sql_id: str, path: Path, sql_text: str) -> CachedSqlEntry:
        checksum = self._compute_checksum(path)
        entry = CachedSqlEntry(
            sql_id=sql_id,
            path=str(path),
            sql_text=sql_text,
            checksum=checksum,
            loaded_at=time.time(),
            access_count=1,
        )
        with self._lock:
            self._put_memory(entry)
            self._disk_path(sql_id).write_text(json.dumps(asdict(entry), ensure_ascii=False, indent=2), encoding="utf-8")
            self._save_manifest()
        return entry

    def invalidate(self, sql_id: str) -> None:
        key = sql_id.lower()
        with self._lock:
            self._memory.pop(key, None)
            disk_path = self._disk_path(sql_id)
            if disk_path.exists():
                disk_path.unlink()
            self._save_manifest()

    def invalidate_all(self) -> None:
        with self._lock:
            self._memory.clear()
            for file in self.cache_root.glob("*.json"):
                if file.name == self._manifest_path.name:
                    continue
                file.unlink()
            self._save_manifest()

    def stats(self) -> dict[str, Any]:
        with self._lock:
            total = self._hits + self._misses
            return {
                "memory_entries": len(self._memory),
                "max_memory_entries": self.max_memory_entries,
                "ttl_seconds": self.ttl_seconds,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": (self._hits / total) if total else 0.0,
            }


_DEFAULT_CACHE: SqlCatalogCache | None = None


def get_sql_catalog_cache() -> SqlCatalogCache:
    global _DEFAULT_CACHE
    if _DEFAULT_CACHE is None:
        _DEFAULT_CACHE = SqlCatalogCache()
    return _DEFAULT_CACHE
