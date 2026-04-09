from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any

_WRITE_LOCK = Lock()


def _root_dir() -> Path:
    return Path(__file__).resolve().parents[2]


def caminho_log_performance() -> Path:
    path = _root_dir() / "logs" / "performance" / "perf_events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _serializar_valor(valor: Any) -> Any:
    if isinstance(valor, Path):
        return str(valor)
    if isinstance(valor, dict):
        return {str(chave): _serializar_valor(conteudo) for chave, conteudo in valor.items()}
    if isinstance(valor, (list, tuple, set)):
        return [_serializar_valor(item) for item in valor]
    if isinstance(valor, (str, int, float, bool)) or valor is None:
        return valor
    return str(valor)


def registrar_evento_performance(
    evento: str,
    duracao_s: float | None = None,
    contexto: dict[str, Any] | None = None,
    status: str = "ok",
) -> None:
    registro: dict[str, Any] = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "evento": str(evento),
        "status": str(status),
    }
    if duracao_s is not None:
        registro["duracao_s"] = round(float(duracao_s), 6)
    if contexto:
        registro["contexto"] = _serializar_valor(contexto)

    try:
        destino = caminho_log_performance()
        with _WRITE_LOCK:
            with destino.open("a", encoding="utf-8") as arquivo:
                arquivo.write(json.dumps(registro, ensure_ascii=False) + "\n")
    except Exception:
        # Instrumentacao de performance nunca deve interromper o fluxo principal.
        return
