from pathlib import Path

import backend.services.pipeline_runtime as pipeline_runtime
from extracao.extracao_oracle_eficiente import ConsultaSql, ResultadoConsultaExtracao


def test_servico_extracao_aborta_quando_todas_as_consultas_falham(monkeypatch, tmp_path: Path):
    consulta = ConsultaSql(
        caminho=tmp_path / "sql" / "reg_0000.sql",
        raiz_sql=tmp_path / "sql",
    )
    consulta.caminho.parent.mkdir(parents=True, exist_ok=True)
    consulta.caminho.write_text("select 1 from dual")

    def fake_execucao(**_kwargs):
        return [
            ResultadoConsultaExtracao(
                consulta=consulta,
                ok=False,
                erro="Credenciais Oracle ausentes.",
            )
        ]

    monkeypatch.setattr(pipeline_runtime, "executar_extracao_oracle", fake_execucao)

    servico = pipeline_runtime.ServicoExtracao(consultas_dir=tmp_path / "sql", cnpj_root=tmp_path / "dados")

    try:
        servico.executar_consultas("12345678000190", [consulta.caminho])
    except RuntimeError as exc:
        mensagem = str(exc)
        assert "Extracao Oracle sem sucesso" in mensagem
        assert "Credenciais Oracle ausentes" in mensagem
    else:
        raise AssertionError("Era esperado abortar a extracao sem arquivos validos.")
