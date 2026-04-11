from __future__ import annotations

import json
from urllib.request import Request, urlopen


API_BASE = "http://localhost:8000"


def _get(path: str) -> dict | str:
    with urlopen(f"{API_BASE}{path}", timeout=5) as response:
        payload = response.read().decode("utf-8")
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return payload


def _post(path: str, payload: dict) -> dict | str:
    request = Request(
        f"{API_BASE}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=5) as response:
        body = response.read().decode("utf-8")
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return body


if __name__ == "__main__":
    print("== health ==")
    print(json.dumps(_get("/api/health"), indent=2, ensure_ascii=False))

    print("\n== observabilidade/status ==")
    print(json.dumps(_get("/api/observabilidade/status"), indent=2, ensure_ascii=False))

    print("\n== observabilidade/stack-smoke ==")
    print(json.dumps(_get("/api/observabilidade/stack-smoke"), indent=2, ensure_ascii=False))

    print("\n== observabilidade/openlineage/test ==")
    print(
        json.dumps(
            _post(
                "/api/observabilidade/openlineage/test",
                {"cnpj": "00000000000000", "job_name": "audit_react.smoke"},
            ),
            indent=2,
            ensure_ascii=False,
        )
    )
