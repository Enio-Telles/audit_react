from pathlib import Path
import sys


sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica.services.dossie_convergencia_report import extrair_resumo_relatorio_contato
from interface_grafica.services.dossie_convergencia_report import diagnosticar_evidencia_contato
from interface_grafica.services.dossie_convergencia_report import gerar_relatorio_mestre_convergencia


def test_extrair_resumo_relatorio_contato_ler_campos_chave(tmp_path):
    caminho_relatorio = tmp_path / "relatorio.md"
    caminho_relatorio.write_text(
        "\n".join(
            [
                "# Relatorio de Comparacao da Secao Contato - 12345678000190",
                "",
                "- Ultimo status de comparacao: `divergencia_funcional`",
                "- Ultima estrategia executada: `sql_consolidado`",
                "- Ultima SQL principal: `dossie_contato.sql`",
                "- Ultimo cache comparado: `cache_a2`",
            ]
        ),
        encoding="utf-8",
    )

    resumo = extrair_resumo_relatorio_contato("12345678000190", caminho_relatorio)

    assert resumo.cnpj == "12345678000190"
    assert resumo.ultimo_status == "divergencia_funcional"
    assert resumo.ultima_estrategia == "sql_consolidado"
    assert resumo.ultima_sql_principal == "dossie_contato.sql"
    assert resumo.ultimo_cache_key == "cache_a2"


def test_gerar_relatorio_mestre_convergencia_consolida_comparacao_e_relatorio_contato(tmp_path):
    raiz_cnpj = tmp_path / "CNPJ"
    comparacao_json = tmp_path / "comparacao.json"
    caminho_saida = tmp_path / "saida" / "relatorio_mestre.md"

    comparacao_json.write_text(
        """
        {
          "cnpjs": {
            "12345678000190": {
              "somente_antes": ["arquivos_parquet/antigo.parquet"],
              "somente_depois": [],
              "alterados": [{"caminho_relativo": "arquivos_parquet/novo.parquet", "divergencias": {"linhas": {"antes": 1, "depois": 2}}}],
              "convergencia_total": false
            },
            "98765432000100": {
              "somente_antes": [],
              "somente_depois": [],
              "alterados": [],
              "convergencia_total": true
            }
          }
        }
        """.strip(),
        encoding="utf-8",
    )

    caminho_relatorio_contato = (
        raiz_cnpj
        / "12345678000190"
        / "arquivos_parquet"
        / "dossie"
        / "relatorio_comparacao_contato_12345678000190.md"
    )
    caminho_relatorio_contato.parent.mkdir(parents=True, exist_ok=True)
    caminho_relatorio_contato.write_text(
        "\n".join(
            [
                "# Relatorio de Comparacao da Secao Contato - 12345678000190",
                "",
                "- Ultimo status de comparacao: `divergencia_funcional`",
                "- Ultima estrategia executada: `sql_consolidado`",
                "- Ultima SQL principal: `dossie_contato.sql`",
                "- Ultimo cache comparado: `cache_a2`",
            ]
        ),
        encoding="utf-8",
    )

    conteudo = gerar_relatorio_mestre_convergencia(comparacao_json, raiz_cnpj, caminho_saida)

    assert caminho_saida.exists()
    assert "# Relatorio Mestre de Convergencia do Plano" in conteudo
    assert "CNPJ 12345678000190" in conteudo
    assert "Convergencia total da extracao: `nao`" in conteudo
    assert "Relatorio tecnico da secao contato" in conteudo
    assert "dossie_contato.sql" in conteudo
    assert "CNPJ 98765432000100" in conteudo
    assert "Convergencia total da extracao: `sim`" in conteudo


def test_diagnosticar_evidencia_contato_identifica_diretorio_ausente(tmp_path):
    raiz_cnpj = tmp_path / "CNPJ"

    evidencia = diagnosticar_evidencia_contato(raiz_cnpj, "12345678000190")

    assert evidencia.status == "diretorio_dossie_ausente"
    assert "ainda nao materializado" in evidencia.detalhe
    assert evidencia.resumo is None


def test_gerar_relatorio_mestre_convergencia_explica_ausencia_de_evidencia_contato(tmp_path):
    raiz_cnpj = tmp_path / "CNPJ"
    comparacao_json = tmp_path / "comparacao.json"
    caminho_saida = tmp_path / "saida" / "relatorio_mestre.md"

    comparacao_json.write_text(
        """
        {
          "cnpjs": {
            "12345678000190": {
              "somente_antes": [],
              "somente_depois": [],
              "alterados": [],
              "convergencia_total": true
            }
          }
        }
        """.strip(),
        encoding="utf-8",
    )

    conteudo = gerar_relatorio_mestre_convergencia(comparacao_json, raiz_cnpj, caminho_saida)

    assert "Evidencia da secao contato: `diretorio_dossie_ausente`" in conteudo
    assert "Detalhe da secao contato: `Diretorio do Dossie ainda nao materializado para o CNPJ.`" in conteudo
