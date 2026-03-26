from fastapi.testclient import TestClient
from api import app

client = TestClient(app)

def test_health_check():
    """Valida se o endpoint de health check retorna status 200 e a versão correta."""
    resposta = client.get("/api/health")
    assert resposta.status_code == 200
    assert resposta.json() == {"status": "ok", "version": "1.0.0"}
