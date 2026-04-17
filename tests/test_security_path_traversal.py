import pytest
from pathlib import Path
from fastapi.testclient import TestClient

# Mock environments to load main app
import sys
sys.path.insert(0, str(Path("backend").resolve()))
from main import app

client = TestClient(app)

def test_parquet_query_path_traversal():
    # Attempt path traversal
    payload = {
        "path": "../../etc/passwd",
        "filters": [],
        "visible_columns": [],
        "page": 1,
        "page_size": 10
    }

    response = client.post("/api/parquet/query", json=payload)

    # In secure implementation, it should throw 400 Bad Request
    assert response.status_code == 400
    assert "Caminho" in response.json()["detail"]
