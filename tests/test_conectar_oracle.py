from pathlib import Path

import utilitarios.conectar_oracle as conectar_oracle


def test_listar_caminhos_env_oracle_prioriza_env_local(monkeypatch, tmp_path: Path):
    env_local = tmp_path / ".env"
    env_externo = tmp_path / "externo.env"

    monkeypatch.setattr(conectar_oracle, "ENV_PATH", env_local)
    monkeypatch.setattr(conectar_oracle, "EXTERNAL_ENV_CANDIDATES", (env_externo,))
    monkeypatch.setattr(conectar_oracle.Path, "cwd", staticmethod(lambda: tmp_path))
    monkeypatch.delenv("AUDIT_REACT_ORACLE_ENV_PATH", raising=False)

    candidatos = conectar_oracle.listar_caminhos_env_oracle()

    assert candidatos[0] == env_local
    assert candidatos[1] == env_externo
