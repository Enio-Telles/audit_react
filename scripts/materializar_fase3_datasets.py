"""
Materializa dependencias e composicoes da Fase 3 por CNPJ.

Uso:
    python scripts/materializar_fase3_datasets.py <CNPJ> [<CNPJ> ...] [--data-limite=DD/MM/AAAA]
"""
from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_SRC_ROOT = _PROJECT_ROOT / "src"
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from extracao.extracao_oracle_eficiente import executar_extracao_oracle
from interface_grafica.services.sql_service import SqlService
from transformacao.composicao_dif_icms import atualizar_composicao_dif_icms
from transformacao.composicao_enderecos import atualizar_composicao_enderecos
from transformacao.composicao_fronteira import atualizar_composicao_fronteira
from utilitarios.dataset_registry import criar_metadata
from utilitarios.dataset_registry import registrar_dataset


def _normalizar_cnpj(cnpj: str) -> str:
    return "".join(caractere for caractere in str(cnpj) if caractere.isdigit())


def _extrair_data_limite(argumentos: list[str]) -> tuple[list[str], str | None]:
    data_limite = None
    cnpjs: list[str] = []
    for argumento in argumentos:
        if argumento.startswith("--data-limite="):
            data_limite = argumento.split("=", 1)[1].strip() or None
            continue
        cnpjs.append(argumento)
    return cnpjs, data_limite


def materializar_dataset_cadastral(cnpj: str) -> object:
    """
    Atualiza o dataset `cadastral` via SQL canonica.

    Este fluxo usa `SqlService` porque `dados_cadastrais.sql` trabalha com o
    bind `:CO_CNPJ_CPF`, diferente das extracoes em lote orientadas a `:CNPJ`.
    """

    sql = SqlService.read_sql("dados_cadastrais.sql")
    registros = SqlService.executar_sql(sql, params={"CO_CNPJ_CPF": cnpj})
    dataframe = SqlService.construir_dataframe_resultado(registros)
    metadata = criar_metadata(
        cnpj=cnpj,
        dataset_id="cadastral",
        sql_id="dados_cadastrais.sql",
        parametros={
            "CO_CNPJ_CPF": cnpj,
            "tabela_origem": [
                "BI.DM_PESSOA",
                "BI.DM_LOCALIDADE",
                "BI.DM_REGIME_PAGTO_DESCRICAO",
                "BI.DM_SITUACAO_CONTRIBUINTE",
                "SITAFE.SITAFE_HISTORICO_GR_SITUACAO",
                "SITAFE.SITAFE_HISTORICO_SITUACAO",
            ],
            "observacao": (
                "Dataset cadastral detalhado para viabilizar composicao "
                "Polars de enderecos sem degradar o contrato legado."
            ),
        },
        linhas=dataframe.height,
    )
    return registrar_dataset(cnpj, "cadastral", dataframe, metadata=metadata)


def materializar_cnpj(cnpj: str, data_limite_processamento: str | None = None) -> None:
    cnpj_limpo = _normalizar_cnpj(cnpj)
    print(f"\n=== Materializacao Fase 3 - {cnpj_limpo} ===")
    if data_limite_processamento:
        print(f"data_limite_processamento={data_limite_processamento}")

    print(f"cadastral={materializar_dataset_cadastral(cnpj_limpo)}")

    resultados_extracao = executar_extracao_oracle(
        cnpj_input=cnpj_limpo,
        consultas_selecionadas=["c190.sql", "shared_sql/sitafe_nfe_calculo_item.sql"],
        max_workers=1,
        tamanho_lote=10000,
    )
    for resultado in resultados_extracao:
        print(
            {
                "sql": str(resultado.consulta.caminho_relativo),
                "ok": resultado.ok,
                "linhas": resultado.linhas,
                "ignorada": resultado.ignorada,
                "arquivo": str(resultado.arquivo_saida) if resultado.arquivo_saida else None,
                "erro": resultado.erro,
            }
        )

    print(f"dif_icms_nfe_efd={atualizar_composicao_dif_icms(cnpj_limpo)}")
    print(
        "composicao_fronteira="
        f"{atualizar_composicao_fronteira(cnpj_limpo, data_limite_processamento=data_limite_processamento)}"
    )
    print(f"composicao_enderecos={atualizar_composicao_enderecos(cnpj_limpo)}")


def main() -> None:
    argumentos = sys.argv[1:]
    if not argumentos:
        print("Uso: python scripts/materializar_fase3_datasets.py <CNPJ> [<CNPJ> ...] [--data-limite=DD/MM/AAAA]")
        raise SystemExit(1)

    cnpjs, data_limite = _extrair_data_limite(argumentos)
    if not cnpjs:
        print("Nenhum CNPJ informado.")
        raise SystemExit(1)

    for cnpj in cnpjs:
        materializar_cnpj(cnpj, data_limite_processamento=data_limite)


if __name__ == "__main__":
    main()
